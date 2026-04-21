# gdrive-fuse

A C++20 implementation of a Google Drive FUSE filesystem with modern C++ design,
ETag-based caching, and a background change watcher.

## Features

- **OAuth2 Authorization Code Flow**: Browser-based authentication with automatic local callback server
- **Google Drive REST API v3**: Full integration for listing, reading, uploading, and deleting files
- **FUSE3 Support**: Mount Google Drive as a local filesystem
- **3-State Directory Cache**: `INVALID → FRESH → STALE` cycle with ETag revalidation to minimise API calls
- **Metadata Cache**: Per-file attribute cache populated during `readdir` — no extra API calls for `getattr`
- **Background Change Watcher**: Polls the Drive Changes API every 30 s and auto-invalidates stale cache entries
- **Google Workspace shortcuts**: Docs, Sheets, Slides, etc. are exposed as `.desktop` files that open the browser directly
- **Thread-Safe Design**: All operations protected with `std::mutex` / `std::atomic`
- **Modern C++20**: Smart pointers, structured bindings, `std::optional`, `std::condition_variable`

## Architecture

The project is structured into four main components:

1. **Auth** (`src/Auth.cpp`)
   - OAuth2 Authorization Code Flow with a temporary local HTTP server on `localhost:8080`
   - Automatic token refresh; tokens persisted in `.gdrive_tokens.json`

2. **GClient** (`src/GClient.cpp`)
   - Google Drive REST API wrapper (list, download, upload, delete)
   - `listFiles()` returns `DirListing{files, etag}`
   - `revalidateDir()` sends `If-None-Match` and returns `std::nullopt` on HTTP 304 (cache still valid)
   - Background change watcher thread: `startChangeWatcher()` / `stopChangeWatcher()`

3. **FuseOps** (`src/FuseOps.cpp`)
   - Implements `getattr`, `readdir`, `read`
   - 3-state directory cache (`DirCacheState`: `INVALID`, `FRESH`, `STALE`) with 30 s TTL
   - Metadata cache populated during `readdir` — `getattr` never needs a separate API call
   - Google Workspace files (`application/vnd.google-apps.*`) displayed as `.desktop` shortcuts
   - `invalidateDirCache()` called by the change watcher callback

4. **main** (`src/main.cpp`)
   - CLI argument parsing
   - Authentication orchestration
   - FUSE initialisation and main loop

## Dependencies

- **FUSE3** (`libfuse3-dev`) — system package, must be installed manually
- **OpenSSL** (`libssl-dev`) — system package, must be installed manually
- **cpr** 1.10.5, **nlohmann/json** 3.11.3, **spdlog** 1.13.0 — fetched automatically via CMake FetchContent

## Building

### Prerequisites

```bash
# Ubuntu / Debian / Linux Mint
sudo apt-get install build-essential cmake pkg-config libfuse3-dev libssl-dev

# Fedora / RHEL
sudo dnf install gcc-c++ cmake pkgconfig fuse3-devel openssl-devel
```

See [docs/BUILD.md](docs/BUILD.md) for full instructions including CI setup.

### Quick start

```bash
git clone https://github.com/the78mole/gdrive-fuse.git
cd gdrive-fuse
make build          # Debug build → build/gdrive-fuse
make install-hooks  # Install pre-commit + commit-msg hooks
```

## Usage

### Setup Google Cloud OAuth2 Credentials

1. Create a project in the [Google Cloud Console](https://console.cloud.google.com/) and enable the **Google Drive API**.
2. Go to **APIs & Services → OAuth consent screen** → set audience to **External** and add your Google account as a **Test user**.
3. Go to **APIs & Services → Credentials → + Create Credentials → OAuth client ID** → Application type: **Desktop app**.
4. Download the JSON and save it as `credentials.json` in the project root.

### Run

```bash
# With Make (default mount point: /home/mnt/gdrive-fuse)
make run CLIENT_ID=<your-client-id> CLIENT_SECRET=<your-client-secret>

# Custom mount point
make run CLIENT_ID=<id> CLIENT_SECRET=<secret> MOUNT_POINT=/mnt/gdrive

# Directly with debug logging
mkdir -p /tmp/gdrive-mount
./build/gdrive-fuse --client-id <id> --client-secret <secret> --debug /tmp/gdrive-mount -f
```

On **first run** a browser window opens automatically for OAuth2 authorisation.
The access token is cached in `.gdrive_tokens.json` and refreshed automatically.

### Google Workspace files (Docs, Sheets, Slides …)

Files with a `application/vnd.google-apps.*` MIME type have no binary content.
They appear in the filesystem with a `.desktop` suffix. Double-clicking them in
a file manager opens the document directly in your browser.

### Unmount

```bash
make stop
# or manually
fusermount3 -u /home/mnt/gdrive-fuse
```

## Thread Safety

| Component | Mechanism |
|---|---|
| `Auth` | `std::mutex` around token state |
| `GClient` | `std::mutex` around API requests; `std::atomic<bool>` + `std::condition_variable` for the watcher thread |
| `FuseOps` | `std::mutex` around all cache structures |

FUSE may dispatch callbacks concurrently from multiple threads.

## Security

- Tokens are stored in `.gdrive_tokens.json` — protect with `chmod 600`.
- Both `credentials.json` and `.gdrive_tokens.json` are in `.gitignore` and must never be committed.

## License

MIT License — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contribution guide including
coding guidelines, commit message format, and PR process.
