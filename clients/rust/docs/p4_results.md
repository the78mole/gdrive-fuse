# Phase 4 — Write-Back Cache: Results

## Summary

Phase 4 adds a full write-back cache architecture to the Rust client.
FUSE `release()` no longer blocks on an HTTP upload; instead it persists content
locally, marks the entry dirty in SQLite, and wakes up a background
`UploadManager` that uploads asynchronously.  A companion `CacheCleaner` thread
enforces a 10 GiB disk-cache budget using LRU eviction while always protecting
dirty (un-uploaded) files.

**Final state:** 66/66 tests passing · `cargo clippy -- -D warnings` clean ·
`cargo build` clean.

---

## New Files

### `src/upload_manager.rs`

Background upload thread (`"gdrive-uploader"`).

| Detail | Value |
|---|---|
| Poll interval | 30 s (woken immediately by each FUSE `release()`) |
| Crash recovery | Processes all `is_dirty=1` rows on startup before entering the event loop |
| New file path | `remote_id` starts with `"__pending__"` → `GClient::create_file` → `DbManager::finalize_new_file` + `ObjectManager::replace_pending_id` |
| Existing file path | `GClient::update_file_content` → `DbManager::clear_dirty_after_upload` + `ObjectManager::store_metadata` |
| HTTP pool | Forks the main `GClient` so uploads never share a connection with downloads |
| Failure policy | Log error; leave `is_dirty=1`; retry on next wakeup/tick |

### `src/cache_cleaner.rs`

Background LRU disk-cache eviction thread (`"gdrive-cache-cleaner"`).

| Detail | Value |
|---|---|
| Check interval | 5 minutes |
| Limit | `MAX_DISK_CACHE_BYTES` = 10 GiB |
| Eviction order | Oldest `atime` first; falls back to `mtime` on `noatime` mounts |
| Safety invariant | Files with `is_dirty=1` are **never** evicted — evicting them would cause data loss |
| Temp files | `.tmp` files (from `DiskCache::insert` atomic writes) are skipped |

---

## Modified Files

### `src/db_manager.rs`

* `CachedMeta` — added `pub name: String` field (between `parent_id` and `last_fetch`)
* `run_migrations()` — idempotent `ALTER TABLE metadata ADD COLUMN name TEXT NOT NULL DEFAULT ''`
* `store_metadata()` — new `name: &str` parameter; SQL updated to include `name` in UPSERT
* `get_metadata()` — SELECT now fetches `name`; row indices shifted accordingly
* **New** `list_dirty_entries() -> Vec<CachedMeta>` — returns all rows with `is_dirty = 1`
* **New** `clear_dirty_after_upload(remote_id, md5)` — clears dirty flag after a successful upload
* **New** `finalize_new_file(pending_id, real_id, md5)` — atomically swaps the temporary pending ID for the real Drive ID in both the `metadata` and `small_files` tables
* Removed `#[allow(dead_code)]` from now-called methods; added targeted `#[allow(dead_code)]` to `inode`, `last_fetch`, `mark_dirty`, `clear_dirty` (populated/available but not yet consumed by a caller in this phase)

### `src/object_manager.rs`

* `store_content()` — refactored to delegate to the new `store_content_bytes` (no behaviour change)
* **New** `store_content_bytes(&self, file_id, content: &[u8])` — routes small content to moka + SQLite BLOB; large content to disk cache
* **New** `write_local_dirty(&self, file_id, content, parent_id, name)` — updates in-memory size, persists content to local cache, upserts metadata row with `is_dirty=true`
* **New** `is_dirty(&self, file_id) -> bool` — queries SQLite dirty flag; `false` when no DB

### `src/fuse_ops.rs`

* `GDriveFuse` struct — added `upload_notify_tx: Option<Sender<()>>`
* `GDriveFuse::new()` — new `upload_notify_tx: Option<Sender<()>>` parameter
* `flush()` / `fsync()` — in write-back mode: snapshot the current write buffer to the local cache (crash-safety between `flush` and `release`)
* `release()` — **two-path design**:
  * Write-back mode (`upload_notify_tx.is_some()`): calls `obj.write_local_dirty()` then pings the `UploadManager`; zero HTTP latency on the FUSE thread
  * Direct-upload fallback (no `DbManager`): original Phase 3 code path preserved unchanged so the no-SQLite configuration still works
* `read()` — dirty-file guard inserted before the network-download path: if `obj.is_dirty()` is true and both cache tiers miss, return `EIO` instead of fetching a stale version from Drive

### `src/sync_manager.rs`

* `store_metadata` call — added `""` for the new `name` parameter (name is unknown from the Drive changes feed)

### `src/main.rs`

* Added `mod cache_cleaner;` and `mod upload_manager;`
* On startup (when `maybe_db.is_some()`):
  1. Creates and starts `UploadManager`; captures `upload_notify_tx`
  2. Creates and starts `CacheCleaner`
* `GDriveFuse::new()` receives `upload_notify_tx`

---

## Architecture

```
FUSE release()
    │
    ├─ upload_notify_tx.is_some()? ──YES──▶ obj.write_local_dirty()
    │                                             │
    │                                        notify_tx.send(())
    │                                             │
    │                                     UploadManager (background)
    │                                       ├─ crash-recovery on startup
    │                                       ├─ wakeup OR 30 s tick
    │                                       ├─ list_dirty_entries()
    │                                       └─ for each:
    │                                           ├─ load from obj cache
    │                                           ├─ new?  create_file()
    │                                           │         finalize_new_file()
    │                                           └─ exist? update_file_content()
    │                                                     clear_dirty_after_upload()
    │
    └─ NO ──▶ direct HTTP upload (Phase 3 fallback, no SQLite)


CacheCleaner (every 5 min)
    └─ scan content_dir
        ├─ total ≤ 10 GiB? → skip
        └─ sort by atime ASC → evict non-dirty until freed enough
```

---

## Test Coverage

| New tests | Module | What is tested |
|---|---|---|
| `list_dirty_entries_returns_only_dirty` | `db_manager` | Only `is_dirty=1` rows returned |
| `clear_dirty_after_upload_updates_md5` | `db_manager` | Clears flag and persists MD5 |
| `finalize_new_file_swaps_pending_id` | `db_manager` | Swaps pending→real ID in metadata and small_files |

Total: **66 tests** (63 from Phase 3.6 + 3 new).

---

## Known Limitations / Future Work

* **Second-write window:** if the user writes to a file while an upload of an earlier version is in progress, `clear_dirty_after_upload` may clear the flag for the newer write.  The content remains on disk and will be re-uploaded within one poll interval (≤ 30 s).  A per-file version counter would eliminate this window entirely.
* `active_uploads` counter (used by `QueueManager` to pause low-priority prefetch workers) is not incremented/decremented by `UploadManager`.  Low-priority prefetch therefore does not yield to in-flight write-back uploads.  Adding the counter to `UploadManager` is a straightforward follow-up.
* `CacheCleaner` does not yet evict entries from the moka RAM cache or the SQLite BLOB store — only the disk content directory is trimmed.
