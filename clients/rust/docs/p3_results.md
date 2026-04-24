# Phase 3 Results — Persistent SQLite Cache + Change-Watcher

## Summary

Phase 3 added a persistent SQLite-backed cache layer and a background
change-watcher that keeps all in-memory caches in sync with Google Drive.
The filesystem survives process restarts without re-downloading small files,
and content is automatically invalidated within 30 seconds of a remote change.

All **63 tests pass** (up from 51 after Phase 2).

---

## New Files

### `src/db_manager.rs`

SQLite cache backed by an `r2d2` connection pool (8 connections, WAL mode).

| Table | Purpose |
|---|---|
| `metadata` | Inode, parent ID, last-fetch timestamp, MD5 checksum, dirty flag |
| `small_files` | Raw BLOB content for files ≤ 4 KiB |
| `sync_state` | Drive changes `startPageToken` (single row) |

Key design decisions:
- **WAL + `synchronous=NORMAL`** — concurrent reads + single writer; crash-safe without full `fsync` on every write.
- **Non-fatal errors** — `DbManager::new()` returns `Result<Arc<Self>>`; `main.rs` uses `.ok()` so a missing or unwritable SQLite file never prevents mounting.
- **`run_gc(content_dir)`** — removes flat content files from previous runs that no longer have a metadata row (orphans created by interrupted downloads or deleted-file changes).

Tests: 12 unit tests in `db_manager::tests`.

### `src/sync_manager.rs`

Background thread (`gdrive-sync`) that polls the Drive changes feed.

**Protocol:**
1. On first start, fetches `startPageToken` via `GET /changes/startPageToken`.
2. Every 30 s: calls `GClient::get_changes(token)`.
3. For each `ChangeItem`:
   - **Removed** → evict moka + disk cache + metadata; delete DB row.
   - **Changed, same MD5** → update metadata only (no content eviction).
   - **Changed, new MD5** → evict all content caches + delete DB row; update metadata and re-insert DB row.  Marks parent directories `Stale` so the next `readdir` revalidates with `If-None-Match`.
4. Persists the new page token in `sync_state`.
5. **Token expiry recovery**: on a 400/410 error, immediately fetches a fresh `startPageToken` and continues.

The thread is only started when a `DbManager` is available.

---

## Modified Files

### `Cargo.toml`

Added three new dependencies:

```toml
rusqlite = { version = "0.31", features = ["bundled"] }
r2d2 = "0.8"
r2d2_sqlite = "0.24"
```

`features = ["bundled"]` compiles SQLite from source — no system library dependency.

### `src/gclient.rs`

| Change | Detail |
|---|---|
| `FileInfo.md5_checksum` | New `#[serde(default)] pub md5_checksum: Option<String>` field.  `None` for folders and Workspace files (no binary representation). |
| 5 × fields query | All Drive API `?fields=` strings now include `md5Checksum`. |
| `ChangeItem` struct | New `pub struct ChangeItem { file_id, removed, file }`. |
| `get_start_page_token()` | `GET /changes/startPageToken` → `String`. |
| `get_changes(token)` | `GET /changes?pageToken=…` → `(Vec<ChangeItem>, new_token)`.  Follows `nextPageToken` pages automatically. |

### `src/object_manager.rs`

| Change | Detail |
|---|---|
| `db: Option<Arc<DbManager>>` | New private field; `None` when SQLite is unavailable. |
| XDG cache path | `new()` now uses `~/.cache/gdrive-fuse-rs/content/` instead of `~/.gdrive/cache/`. |
| `new_with_db(db)` | New pub constructor — XDG content dir + active `DbManager`. |
| `new_for_test(dir)` | Now creates a real `DbManager` in `dir/metadata.db` for hermetic DB tests. |
| `store_content()` | Files ≤ 4 KiB → moka **and** `db.store_small_file()`; larger files → disk unchanged. |
| `get_content()` | Lookup order: moka → `db.get_small_file()` (warms moka on hit) → `None`. |
| `evict_metadata(id)` | New pub method — removes in-memory metadata entry. |
| `evict_disk_content(id)` | New pub method — removes flat content file. |
| `mark_dir_stale_for_file(id)` | New pub method — sets `DirCacheState::Stale` on every dir entry containing `id`. |

### `src/fuse_ops.rs`

Added `md5_checksum: None` to the `FileInfo` placeholder in `create()`.

### `src/main.rs`

```
mod db_manager;
mod sync_manager;
```

Startup sequence after the GClient is built:
1. Compute `~/.cache/gdrive-fuse-rs/metadata.db` and `content/` paths.
2. `DbManager::new()` — `.ok()` makes failure non-fatal.
3. `db.run_gc(&content_dir)` — remove orphaned content files.
4. `ObjectManager::new_with_db(db)` or `ObjectManager::new()` depending on availability.
5. After `GDriveFuse` is constructed, start `SyncManager` if DB is present.

---

## Cache Architecture (post Phase 3)

```
┌────────────────────────────────────────────────────────┐
│  FUSE read() / getattr() / readdir()                   │
└──────────────────┬─────────────────────────────────────┘
                   │
          ┌────────▼────────┐
          │   ObjectManager  │
          │                  │
          │  ┌─────────────┐ │   hit → Arc clone
          │  │  moka RAM   │◄├──── get_content()
          │  │  (256 MiB)  │ │
          │  └──────┬──────┘ │
          │         │ miss   │
          │  ┌──────▼──────┐ │   hit → warms moka
          │  │  SQLite DB  │◄├──── small_files BLOB
          │  │  (≤4 KiB)   │ │
          │  └──────┬──────┘ │
          │         │ miss   │
          │  ┌──────▼──────┐ │   seek+read, no full load
          │  │  Disk cache │◄├──── read_disk_slice()
          │  │ ~/.cache/…  │ │   (4 KiB < size < 64 MiB)
          │  └─────────────┘ │
          └────────┬─────────┘
                   │ all misses
          ┌────────▼─────────┐
          │  QueueManager     │
          │  → GClient HTTP   │   streaming for >64 MiB
          └───────────────────┘

  Background thread:
  SyncManager (30 s) ──► GClient /changes ──► evict stale entries
```

---

## Test Results

```
cargo test
...
test result: ok. 63 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

New tests (12) in `db_manager::tests`:
- `store_and_get_metadata_roundtrip`
- `get_metadata_missing_returns_none`
- `metadata_upsert_updates_existing_row`
- `mark_and_clear_dirty`
- `remove_entry_clears_both_tables`
- `small_file_store_and_get_roundtrip`
- `small_file_missing_returns_none`
- `small_file_upsert_replaces_data`
- `sync_token_set_and_get`
- `list_all_remote_ids_empty_and_populated`
- `gc_removes_orphaned_content_files`
- `gc_on_missing_content_dir_is_silent`
