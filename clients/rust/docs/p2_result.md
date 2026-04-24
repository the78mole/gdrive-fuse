# Phase 2 — Moka Cache, Streaming Downloads, Non-blocking `getattr`

## Summary

Phase 2 adds three improvements to the Rust FUSE client that reduce memory
pressure, prevent OOM on large files, and eliminate the last blocking FUSE
callback.

**All 51 tests pass; zero compiler warnings.**

---

## Feature 1 — Moka content cache (`object_manager.rs`, `Cargo.toml`)

### Before

`ContentCache` was a hand-rolled FIFO with `Mutex<HashMap<String, Vec<u8>>>` +
`VecDeque<String>` for insertion-order eviction.  Drawbacks:

- **FIFO, not LFU** — a single large sequential scan evicted all hot entries.
- **`Vec<u8>` values** — every `get_content()` call cloned the full byte
  vector, burning CPU and allocator bandwidth for even a 4 KiB read.
- **Global mutex** — a single lock serialised all concurrent readers.
- 1 GiB cap was a simple byte counter with no TTL.

### After

`ContentCache` is now a thin wrapper around `moka::sync::Cache<String, Arc<Vec<u8>>>`:

| Property | Old | New |
|---|---|---|
| Eviction policy | FIFO | TinyLFU + SLRU (hot entries survive) |
| Value type | `Vec<u8>` (cloned on every read) | `Arc<Vec<u8>>` (one atomic inc per read) |
| Capacity | 1 GiB (simple counter) | 256 MiB (byte-weighted, constant `CACHE_MOKA_MAX_BYTES`) |
| TTL | none | 10 minutes (`CACHE_MOKA_TTL`) |
| Locking | single `parking_lot::Mutex` | moka's internal sharded lock |

`get_content()` now returns `Option<Arc<Vec<u8>>>`.  All callers use `Deref`
to reach `[u8]`, so the change is transparent to the FUSE read path.  The
write-buffer seeding path (`open()` with `O_RDWR`) calls `Arc::unwrap_or_clone`
to obtain an owned `Vec<u8>` — acceptable since writes are rare.

New constants exported from `object_manager`:

```rust
pub const CACHE_MOKA_MAX_BYTES: u64    = 256 * 1024 * 1024; // 256 MiB capacity
pub const CACHE_MOKA_TTL: Duration     = Duration::from_secs(600); // 10 min TTL
pub const CACHE_STREAM_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024; // see Feature 2
```

---

## Feature 2 — Streaming HTTP Range reads for large files (`fuse_ops.rs`, `gclient.rs`)

### Before

Every cache-miss in `read()` enqueued a `TaskKey::DownloadFile` task that
fetched the **entire** file before serving a single `read()` slice.  A 4 GiB
video required 4 GiB of memory just to answer a 128 KiB kernel request.

`GClient::download_file_range()` already existed but was marked
`#[allow(dead_code)]`.

### After

`read()` now checks `FileInfo::size` against `CACHE_STREAM_THRESHOLD_BYTES`
(64 MiB) on the cache-miss path:

- **`size ≤ 64 MiB`** → existing `DownloadFile` queue task → full file
  downloaded once → stored in disk or RAM cache → subsequent reads are free.
- **`size > 64 MiB`** → `client.download_file_range(file_id, offset, size)`
  called directly from the reply-dispatcher pool → only the requested window
  (`[offset, offset+size)`) is transferred → bytes are returned to the kernel
  immediately and **not** stored in any cache.

```
┌────────────────────────────────────────────────────┐
│ read() — cache miss branch                         │
│                                                    │
│  file_size ≤ 64 MiB?                               │
│  ├─ yes → enqueue DownloadFile → serve from cache  │
│  └─ no  → download_file_range → reply.data(&bytes) │
│            (no caching, no OOM risk)               │
└────────────────────────────────────────────────────┘
```

`#[allow(dead_code)]` removed from `download_file_range`.

---

## Feature 3 — Non-blocking `getattr` (`fuse_ops.rs`)

### Before

```
FUSE thread
  └─ getattr (cache miss)
       └─ enqueue_and_wait(GetMetadata, MetaUrgent)   ← BLOCKS fuse thread
```

### After

```
FUSE thread                          reply-dispatcher pool thread
  └─ getattr (cache miss)                └─ enqueue_and_wait(GetMetadata)
       └─ reply_tx.send(closure) ──────►      ├─ obj.get_metadata()
            └─ returns immediately             └─ reply.attr(...)
```

Same `reply_tx` pattern used by `lookup`, `readdir`, and `read`.  The FUSE
event-loop thread is now fully non-blocking for all read-path callbacks.

---

## Files changed

| File | Change |
|---|---|
| `Cargo.toml` | `moka = { version = "0.12", features = ["sync"] }` added |
| `src/object_manager.rs` | `ContentCache` replaced; new constants; `get_content` returns `Arc`; tests updated |
| `src/gclient.rs` | `#[allow(dead_code)]` removed from `download_file_range` |
| `src/fuse_ops.rs` | `CACHE_STREAM_THRESHOLD_BYTES` imported; `read()` streaming branch; `getattr` non-blocking |
| `src/queue_manager.rs` | Test assertion updated for `Arc<Vec<u8>>` return type |

---

## Test results

```
test result: ok. 51 passed; 0 failed; 0 ignored  (cargo test, dev profile)
```
