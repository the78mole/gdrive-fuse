# Phase 6 — Centralized Config & Secure Token Handling

## Goals

| Goal | Status |
|------|--------|
| Centralized TOML config (`~/.config/gdrive-fuse-rs/config.toml`) | ✅ |
| Secure token storage (`~/.local/state/…/token.json`, mode 0600) | ✅ |
| Remove all hard-coded magic-number constants from the public API | ✅ |
| Wire `Arc<Config>` through all managers | ✅ |
| `cargo clippy -- -D warnings` clean | ✅ |
| All 66 tests pass | ✅ |

---

## New module: `src/config_manager.rs`

### Location

`~/.config/gdrive-fuse-rs/config.toml`  (XDG config dir, created on first run)

### Schema

```toml
[cache]
ram_max_bytes          = 4096         # files ≤ this stay in RAM only
moka_max_bytes         = 268435456    # 256 MiB in-process content cache
moka_ttl_secs          = 600          # 10 min TTL for moka entries
disk_max_bytes         = 10737418240  # 10 GiB on-disk cache budget
stream_threshold_bytes = 67108864     # 64 MiB — larger files are streamed
dir_ttl_secs           = 30           # kernel attribute/directory cache TTL

[sync]
interval_secs = 30   # change-watcher polling interval

[log]
level = "info"       # overridden by --debug flag or RUST_LOG env var

[oauth]
client_id     = ""   # optional; fallback credential source
client_secret = ""   # optional; fallback credential source
```

All fields have compile-time defaults; a missing field (or a missing file)
is silently treated as the default value.  An unreadable file logs a warning
and falls back to `Config::default()` so the daemon never refuses to start due
to a config error.

### `ConfigManager::load_or_create()`

1. Resolves the config path via `dirs::config_dir()`.
2. If the file does not exist, creates it with a commented header.
3. Deserialises with `toml::from_str` + `serde` `#[serde(default)]`.
4. Returns `Arc<Config>`.

---

## Secure token storage (`src/auth.rs`)

### New path

`~/.local/state/gdrive-fuse-rs/token.json`  (XDG state dir)

Migration note: the old path was `~/.gdrive_tokens_rs.json`.
Delete the old file manually after upgrading; the daemon will re-authenticate
on next start.

### Permission enforcement

* **Write** (`save_tokens`): parent directory is created with
  `fs::create_dir_all`, then the file is written, then permissions are set to
  `0o600` via `PermissionsExt::set_mode`.
* **Read** (`load_tokens`): `mode & 0o077 != 0` → `bail!` with a clear error
  message and a `chmod 600` hint logged at `warn!` level.

---

## Constants removed from the public API

The following `pub const` items were removed from `object_manager.rs` and
`cache_cleaner.rs`.  Their values are now sourced from `Arc<Config>` at
runtime.

| Removed constant | Config field | Default |
|---|---|---|
| `TTL` (30 s `Duration`) | `cache.dir_ttl_secs` | 30 |
| `CACHE_RAM_MAX_BYTES` | `cache.ram_max_bytes` | 4 096 |
| `CACHE_MOKA_MAX_BYTES` | `cache.moka_max_bytes` | 268 435 456 |
| `CACHE_MOKA_TTL` | `cache.moka_ttl_secs` | 600 |
| `CACHE_STREAM_THRESHOLD_BYTES` | `cache.stream_threshold_bytes` | 67 108 864 |
| `MAX_DISK_CACHE_BYTES` | `cache.disk_max_bytes` | 10 737 418 240 |
| `SYNC_INTERVAL` | `sync.interval_secs` | 30 |

`ROOT_INO` and `INODE_DUPLICATES` are unchanged (structural constants, not
tuning knobs).

---

## Changed constructor signatures

### `ObjectManager`

```rust
// Before
ObjectManager::new()
ObjectManager::new_with_db(db: Arc<DbManager>)

// After
ObjectManager::new()                                           // default config
ObjectManager::new_with_db_and_config(db, config: Arc<Config>) // production
ObjectManager::new_for_test(cache_dir: PathBuf)               // tests (unchanged)
```

`new_with_db` was removed; use `new_with_db_and_config` with `Arc::new(Config::default())` if you need the old behaviour.

### `CacheCleaner`

```rust
// Before
CacheCleaner::new(db: Arc<DbManager>, content_dir: PathBuf)

// After
CacheCleaner::new(db: Arc<DbManager>, content_dir: PathBuf, max_disk_bytes: u64)
```

### `SyncManager`

```rust
// Before
SyncManager::new(db, obj, client)

// After
SyncManager::new(db, obj, client, interval: Duration)
```

---

## Credential priority chain

In `main.rs`, credentials are resolved in this order (first non-empty wins):

1. `--client-id` / `--client-secret` CLI flags
2. `CLIENT_ID` / `CLIENT_SECRET` environment variables
3. `oauth.client_id` / `oauth.client_secret` in `config.toml`
4. Compile-time `option_env!("CLIENT_ID")` / `option_env!("CLIENT_SECRET")`

If none of the above provide a `client_secret`, the daemon exits with a clear
error message.

---

## Log level resolution

1. `--debug` flag → `debug` level, regardless of everything else
2. `RUST_LOG` environment variable (if set)
3. `log.level` in `config.toml`

---

## Test changes

* `ContentCache::new()` in tests now passes explicit `(max_bytes, ttl)` matching
  the `Config::default()` values.
* Tests that used `CACHE_RAM_MAX_BYTES` now define a local
  `const CACHE_RAM_MAX_BYTES: u64 = 4 * 1024;` (matches the default).

---

## Migration guide (existing installations)

1. **Token file moved.**  Delete `~/.gdrive_tokens_rs.json`; the daemon will
   re-authenticate and create `~/.local/state/gdrive-fuse-rs/token.json`
   with mode `0600`.
2. **Config auto-created.**  On first run `~/.config/gdrive-fuse-rs/config.toml`
   is created with defaults.  Edit it to tune cache sizes or the sync interval.
3. **No CLI changes.**  All existing flags continue to work.
