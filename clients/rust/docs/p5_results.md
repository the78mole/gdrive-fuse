# Phase 5 — Content-Addressable Storage (CAS), GC Overhaul & Virtual `.duplicates`

## Summary

Phase 5 converts the local content cache from a `remote_id`-keyed store to a
content-addressable store keyed by **MD5 checksum** for clean files, updates
garbage collection to match the new key space, and exposes a virtual read-only
file `/.duplicates` that lists all files sharing the same MD5 hash.

All 66 tests continue to pass; `cargo clippy -- -D warnings` is clean.

---

## 1. Content-Addressable Storage (CAS)

### Key selection rule

| File state | Cache key |
|---|---|
| `__pending__` prefix (not yet uploaded) | `file_id` |
| `is_dirty = 1` in metadata | `file_id` |
| Clean, MD5 known | `md5_checksum` |
| Clean, MD5 unknown | `file_id` (fallback) |

The helper `ObjectManager::cache_key_for(file_id)` encodes this rule and is
used by all read paths (`get_content`, `has_disk_content`, `read_disk_slice`,
`read_full_disk_content`).

### Why dirty writes bypass CAS

`write_local_dirty`, `FuseOps::flush`, and `FuseOps::fsync` call
`store_content_at_key(file_id, data)` directly, skipping `cache_key_for`.
At the time these functions run the `is_dirty` flag has **not yet been
persisted** to SQLite, so `cache_key_for` would incorrectly resolve to the
MD5 key and the dirty content would overwrite a shared clean blob.

### New `ObjectManager` API

| Method | Purpose |
|---|---|
| `cache_key_for(file_id)` | Private; returns effective cache key for any read |
| `store_content_at_key(key, data)` | Public; bypasses CAS — writes under the supplied key |
| `migrate_cache_key(old, new)` | Public; atomically moves moka + SQLite + disk entry from `old` key to `new` key |

`DiskCache::rename_key` (private) handles the on-disk rename that backs
`migrate_cache_key`.

### Post-upload key migration (`upload_manager.rs`)

After a successful upload the server returns the authoritative MD5.  The
upload manager calls `migrate_cache_key(file_id, md5)` (and
`db.migrate_small_file_key(file_id, md5)` for the SQLite blob store) so that
subsequent reads resolve through the CAS path immediately, without evicting
and re-fetching the content.

This happens for both new-file uploads (after `finalize_new_file`) and
existing-file updates (after `clear_dirty_after_upload`).

### `small_files` table

The `store_small_file` / `get_small_file` methods now accept a generic `key`
parameter (was `remote_id`).  The new helper `migrate_small_file_key(old, new)`
performs an `INSERT OR REPLACE … SELECT … DELETE` pair within the same
connection to avoid a window where neither key exists.

`finalize_new_file` stores the CAS key (`md5.unwrap_or(real_id)`) rather than
the raw `real_id` so the small-files table stays consistent with the disk cache
immediately after the pending-to-real transition.

---

## 2. Garbage Collection overhaul

### Problem with the old approach

The previous `run_gc` called `list_all_remote_ids()` and deleted any disk file
whose name did not appear in that set.  Under CAS, disk files are named by MD5,
so they would never match a `remote_id` — every cached content file would be
deleted on the next GC cycle.

### New bulk-query approach

`run_gc` now builds a **protected set** with two SQL queries:

```sql
-- Protected: all MD5 checksums (CAS-keyed clean files)
SELECT DISTINCT md5_checksum FROM metadata
WHERE md5_checksum IS NOT NULL AND md5_checksum != '';

-- Protected: all dirty remote_ids (file_id-keyed dirty files)
SELECT remote_id FROM metadata WHERE is_dirty = 1;
```

Any file in `content/` whose name is not in the protected set (and is not a
`.tmp` scratch file) is removed.  A single MD5 blob is therefore retained as
long as **any** metadata row still references that checksum — satisfying the
multi-copy deduplication invariant.

`list_all_remote_ids` is retained for its unit test but annotated
`#[cfg_attr(not(test), allow(dead_code))]`.

### `cache_cleaner.rs`

Added an early `continue` guard for files whose `file_id` starts with
`__pending__`, preventing the background eviction loop from expiring content
that has not been uploaded yet.

---

## 3. Virtual `/.duplicates` (inode 3)

### Motivation

When multiple files share the same MD5 they map to the same content blob.
The virtual file surfaces this information so operators can inspect or clean
up duplicate files without external tooling.

### FUSE integration

| Constant | Value |
|---|---|
| `INODE_DUPLICATES` | `3` |

`GDriveFuse` gains an `db: Option<Arc<DbManager>>` field, initialised from
`main.rs` alongside the existing `maybe_db` reference.

FUSE operations that recognise inode 3:

| Operation | Behaviour |
|---|---|
| `lookup` | Intercepts `(".duplicates", parent = ROOT_INO)` and returns `duplicates_attr()` immediately |
| `getattr` | Returns `duplicates_attr()` for inode 3 |
| `open` | Returns `FileHandle(0)` with empty flags |
| `read` | Calls `generate_duplicates_report(db)`, slices `[offset .. offset+size]`, returns data |
| `readdir` (root) | Appends `(INODE_DUPLICATES, RegularFile, ".duplicates")` to the root listing |

`duplicates_attr()` returns a 0-byte, mode `0o444` regular file — the kernel
re-reads the actual byte count from each `read` reply.

### Report format

```
MD5: <hex>
  -> /path/to/first/copy
  -> /path/to/second/copy
…
```

Generated by `generate_duplicates_report(db)` (free function in `fuse_ops.rs`)
using two new `DbManager` methods:

- `get_md5_duplicates()` — `GROUP BY md5_checksum HAVING COUNT(*) > 1`; returns
  `Vec<(String, Vec<(String, String)>)>` (`md5 → [(remote_id, name)]`).
- `resolve_path(remote_id)` — recursive CTE that walks the `parent_id` chain
  upward from the given file, building a `/`-separated absolute path string.

---

## 4. Test changes

| Test | Change |
|---|---|
| `gc_removes_orphaned_content_files` | Content file now uses MD5 key (`deadbeef`); a dirty `remote_id` file is also protected; orphan is deleted |
| `finalize_new_file_swaps_pending_id` | Asserts `get_small_file("abc")` (MD5) has content; asserts `get_small_file("real-drive-id")` is `None` |

---

## 5. Files modified

| File | Nature of change |
|---|---|
| `src/object_manager.rs` | CAS key logic, `store_content_at_key`, `migrate_cache_key`, `DiskCache::rename_key`, `INODE_DUPLICATES` constant |
| `src/db_manager.rs` | `store/get_small_file` key rename, `migrate_small_file_key`, `run_gc` rewrite, `finalize_new_file` CAS key, `get_md5_duplicates`, `resolve_path`, updated tests |
| `src/cache_cleaner.rs` | `__pending__` guard |
| `src/fuse_ops.rs` | `db` field, virtual `.duplicates` handlers (`lookup`, `getattr`, `open`, `read`, `readdir`), `duplicates_attr()`, `generate_duplicates_report()`, `flush`/`fsync` use `store_content_at_key` |
| `src/upload_manager.rs` | Post-upload `migrate_cache_key` + `migrate_small_file_key` calls |
| `src/main.rs` | Pass `maybe_db.clone()` to `GDriveFuse::new()` |
