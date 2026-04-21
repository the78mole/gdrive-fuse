# GitHub Copilot Instructions — gdrive-fuse

## Project Overview

`gdrive-fuse` is a multi-language monorepo that mounts Google Drive as a local
FUSE3 filesystem on Linux. Each language client implements the same feature set:
Google Drive REST API, OAuth2 Authorization Code + token refresh, ETag-based
3-state directory cache, per-file content cache, fine-grained concurrent locking,
and a background change-watcher.

---

## Monorepo Structure

```
clients/
  cpp/    ← C++20 (CMake)  — authoritative reference implementation
  rust/   ← Rust 2021 (Cargo)
benchmarks/
  run.sh  ← performance comparison harness (requires hyperfine)
docs/
.github/
Makefile  ← top-level; delegates to each client
```

### Adding a new language client

1. Create `clients/<lang>/` with its native build system.
2. Implement the following interface (same semantics as the C++ reference):
   - `list_files(parent_id)` → file listing + ETag
   - `revalidate_dir(parent_id, etag)` → `Option<listing>` (None = 304 Not Modified)
   - `get_file_metadata(file_id)` → `FileInfo`
   - `download_file(file_id)` → bytes
   - FUSE operations: `getattr`, `readdir`, `read`
   - OAuth2 Authorization Code + silent token refresh
3. Add `build-<lang>`, `build-<lang>-release`, `run-<lang>`, `lint-<lang>`,
   `format-<lang>` targets to the top-level `Makefile`.
4. Update this file under **Language-specific notes**.

---

## Language & Standard

### C++ (`clients/cpp/`)
- **C++20** — use concepts, ranges, `std::span`, structured bindings,
  `[[nodiscard]]` etc. where they improve clarity.
- Compiler flags: `-Wall -Wextra -Wpedantic -Werror`; no warnings allowed.
- Linux-only POSIX extensions are acceptable where required by FUSE.

### Rust (`clients/rust/`)
- **Edition 2021** — idiomatic Rust; use `?` for error propagation.
- `#![deny(warnings)]` is enforced; `cargo clippy -- -D warnings` must pass.
- Key crates: `fuser` (FUSE3), `reqwest` (blocking), `parking_lot`, `dashmap`.
- No `unsafe` blocks without a documented safety comment.

---

## Build System

| Command | Effect |
|---|---|
| `make build` | Debug build for all clients |
| `make build-cpp` | CMake Debug build + `compile_commands.json` |
| `make build-rust` | `cargo build` (debug) |
| `make build-release` | Release build for all clients |
| `make run-cpp` | Build then mount C++ client at `$HOME/mnt/gdrive-fuse` |
| `make run-rust` | Build then mount Rust client at `$HOME/mnt/gdrive-fuse` |
| `make stop` | `fusermount3 -u` the mount |
| `make format` | `clang-format` + `cargo fmt` on all sources |
| `make lint` | `clang-tidy` + `cargo clippy` |
| `make bench` | Run `benchmarks/run.sh` (requires `hyperfine`) |
| `make install-hooks` | Install `pre-commit` hooks |

Always run `make format` and `make lint` before suggesting a commit.

---

## Commit Messages — Conventional Commits v1.0

Every commit **must** match the pattern enforced by `.githooks/commit-msg`:

```
<type>(<scope>): <imperative description>

[optional body]

[optional footer: BREAKING CHANGE: ..., Fixes #N, Closes #N]
```

### Types and their SemVer impact

| Type | SemVer bump | When to use |
|---|---|---|
| `feat` | **MINOR** | New user-visible feature or capability |
| `fix` | **PATCH** | Bug fix |
| `perf` | **PATCH** | Performance improvement with no API change |
| `refactor` | **PATCH** | Internal restructuring, no behaviour change |
| `docs` | – | Documentation only |
| `style` | – | Formatting, whitespace — no logic change |
| `test` | – | Add or fix tests |
| `chore` | – | Tooling, deps, config that does not affect runtime |
| `ci` | – | CI/CD pipeline changes |
| `build` | – | CMake, Makefile, dependency changes |
| `revert` | varies | Reverts a previous commit |

Append `!` after type/scope to signal a **BREAKING CHANGE** → **MAJOR** bump:

```
feat(auth)!: replace implicit token refresh with explicit re-auth flow

BREAKING CHANGE: callers must now call Auth::refreshIfExpired() manually.
```

### Scopes (use consistently)

`auth` · `gclient` · `fuseops` · `cache` · `watcher` · `build` · `docs` ·
`ci` · `deps`

### Examples

```
feat(cache): add per-file content cache with 64-entry LRU eviction
fix(fuseops): release cache lock before HTTP call in readdir
perf(gclient): remove global mutex, allow parallel API requests
docs(build): document fuse group requirement for non-root mount
chore(deps): bump nlohmann/json to v3.11.3
```

---

## Versioning

This project follows **Semantic Versioning 2.0.0** (`MAJOR.MINOR.PATCH`).
The version is the single source of truth in `CMakeLists.txt`:

```cmake
project(gdrive-fuse VERSION <MAJOR>.<MINOR>.<PATCH> LANGUAGES CXX)
```

- Bump **PATCH** for `fix`, `perf`, `refactor`.
- Bump **MINOR** for `feat` (and reset PATCH to 0).
- Bump **MAJOR** for any breaking change (`!` suffix or `BREAKING CHANGE:`
  footer), and reset MINOR and PATCH to 0.
- Git tags: `vMAJOR.MINOR.PATCH` (e.g. `v1.2.0`).

---

## Architecture & Key Invariants

### Threading model

- `FuseOps::cache_mutex_` (`std::shared_mutex`) — guards `path_to_id_cache_`,
  `metadata_cache_`, `dir_cache_`, `file_content_cache_`.
  - **shared_lock** for pure reads (e.g. `getattr` cache hit).
  - **unique_lock** for any write (cache update, invalidation).
- `FuseOps::file_mutex_registry_mutex_` + `file_mutexes_` — per-file download
  serialisation. Different files download in parallel; the same file is
  serialised. Acquire **after** releasing `cache_mutex_`.
- Never hold `cache_mutex_` during an HTTP call — always release before calling
  into `GClient`.
- `GClient` methods are stateless with respect to locking (no internal mutex);
  `Auth::getAccessToken()` has its own lock.

### Cache lifecycle

```
INVALID → (listFiles) → FRESH
FRESH   → (TTL ≥ 30 s) → STALE
STALE   → (revalidateDir 304) → FRESH        (no content change)
STALE   → (revalidateDir 200) → FRESH        (new listing stored)
any     → (change-watcher fires) → INVALID
```

### Error handling

- FUSE callbacks must never throw across the FUSE boundary. Wrap the entire
  body in `try { … } catch (const std::exception& e) { … return -EIO; }`.
- Return standard POSIX error codes: `-ENOENT`, `-EIO`, `-EACCES`, etc.
- Log with `spdlog::error()` / `spdlog::debug()`.

### Memory ownership

- No raw owning pointers. Use `std::shared_ptr` (`GClient`, `Auth`) or
  `std::unique_ptr` for singletons.
- The `FuseOps::instance_` singleton is set via `setInstance()` before
  `fuse_main` and is valid for the entire FUSE lifetime.

---

## Code Style

- Follow `.clang-format` (LLVM base, 100-col limit, 4-space indent).
- `#pragma once` instead of include guards.
- Group includes: project headers → third-party → standard library, each group
  sorted alphabetically.
- Use `[[nodiscard]]` on functions where ignoring the return value is always
  a bug.
- Prefer `std::string_view` for read-only string parameters.
- Document public API with Doxygen-style `/** */` comments.

---

## Security

- Never log or print OAuth tokens, client secrets, or file content.
- `credentials.json` and `.gdrive_tokens.json` are in `.gitignore` and must
  never be committed.
- Validate all API responses before parsing (check `status_code` first).
- No `system()` / `popen()` calls.

---

## Testing

- Unit tests live in `tests/` (not yet scaffolded; follow GoogleTest
  conventions when added).
- Integration tests require a real (or mocked) Google Drive account.
- Before opening a PR, verify: `make build && make lint`.
