# Build Guide

This document explains how to build, configure, and run **gdrive-fuse** from source.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Getting the Source](#getting-the-source)
3. [Building with Make (recommended)](#building-with-make-recommended)
4. [Building with CMake directly](#building-with-cmake-directly)
5. [Google Cloud Credentials](#google-cloud-credentials)
6. [Running](#running)
7. [Stopping / Unmounting](#stopping--unmounting)
8. [CI / Release Builds](#ci--release-builds)
9. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### System packages

| Distribution | Command |
|---|---|
| Ubuntu / Debian / Linux Mint | `sudo apt-get install build-essential cmake pkg-config libfuse3-dev libssl-dev` |
| Fedora / RHEL / Rocky | `sudo dnf install gcc-c++ cmake pkgconfig fuse3-devel openssl-devel` |
| Arch Linux | `sudo pacman -S base-devel cmake fuse3 openssl` |

Minimum versions:
- **CMake** ≥ 3.20
- **GCC** ≥ 12 or **Clang** ≥ 15 (C++20 required)
- **libfuse3** ≥ 3.10
- **OpenSSL** ≥ 1.1 (needed by the bundled cpr/curl)

### Fetched automatically by CMake

The following libraries are downloaded at configure time via `FetchContent` — no manual installation required:

| Library | Version | Purpose |
|---|---|---|
| [nlohmann/json](https://github.com/nlohmann/json) | 3.11.3 | JSON parsing |
| [spdlog](https://github.com/gabime/spdlog) | 1.13.0 | Structured logging |
| [cpr](https://github.com/libcpr/cpr) | 1.10.5 | HTTP client (wraps curl) |

An active internet connection is required for the first build. Subsequent builds use the CMake cache.

---

## Getting the Source

```bash
git clone https://github.com/the78mole/gdrive-fuse.git
cd gdrive-fuse
```

---

## Building with Make (recommended)

The project ships a `Makefile` that wraps the CMake invocations.

### Debug build

```bash
make build
```

Produces `build/gdrive-fuse` with debug symbols and assertions enabled.

### Release build

For a release build, set `CLIENT_ID` and `CLIENT_SECRET` in the environment
**before** building — they are compiled directly into the binary so end users
do not need to supply them:

```bash
export CLIENT_ID=<your-client-id>
export CLIENT_SECRET=<your-client-secret>
make build-release
```

Or inline:

```bash
make build-release CLIENT_ID=<id> CLIENT_SECRET=<secret>
```

A debug build without credentials is fine for development — credentials can
always be passed at runtime (see [Running](#running)).

### All Make targets

```
make help          – show all targets
make build         – Debug build
make build-release – Release build (requires credentials)
make run           – mount Drive at /home/mnt/gdrive-fuse
make stop          – unmount and stop the process
```

---

## Building with CMake directly

```bash
# Debug
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel $(nproc)

# Release
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel $(nproc)
```

---

## Google Cloud Credentials

gdrive-fuse uses the **OAuth 2.0 Authorization Code Flow** with a local redirect to `http://localhost:8080`.

### Create credentials

1. Open the [Google Cloud Console](https://console.cloud.google.com/) and create a project.
2. Go to **APIs & Services → Library**, search for **Google Drive API** and enable it.
3. Go to **APIs & Services → OAuth consent screen**.
   - Set audience to **External**.
   - Add your Google account as a **Test user** (mandatory while the app is unverified).
4. Go to **APIs & Services → Credentials → + Create Credentials → OAuth client ID**.
   - Application type: **Desktop app**.
5. Download the JSON file and save it as `credentials.json` in the project root.

The file looks like this:

```json
{
  "installed": {
    "client_id": "XXXXXXXXXX.apps.googleusercontent.com",
    "client_secret": "GOCSPX-...",
    ...
  }
}
```

> **Security:** `credentials.json` and `.gdrive_tokens.json` are listed in `.gitignore` and must never be committed.

---

## Running

Credentials are resolved in the following order (highest priority first):

1. `--client-id` / `--client-secret` CLI arguments
2. `CLIENT_ID` / `CLIENT_SECRET` runtime environment variables
3. Values compiled into the binary at build time

```bash
# With Make — release binary already has credentials embedded:
make run

# Override credentials at runtime:
make run CLIENT_ID=<id> CLIENT_SECRET=<secret>

# Custom mount point
make run CLIENT_ID=<id> CLIENT_SECRET=<secret> MOUNT_POINT=/mnt/mydrive

# Directly
mkdir -p /tmp/gdrive-mount
./build/gdrive-fuse \
    --client-id    <id>     \
    --client-secret <secret> \
    --debug \
    /tmp/gdrive-mount -f
```

On **first run** the browser opens automatically for OAuth2 authorisation. The token is cached in `.gdrive_tokens.json` and refreshed automatically on subsequent runs.

### FUSE user permissions

If you get `fusermount3: failed to access mountpoint – Permission denied`, add your user to the `fuse` group:

```bash
sudo usermod -aG fuse "$USER"
# log out and back in, then retry
```

---

## Stopping / Unmounting

```bash
make stop

# or manually
fusermount3 -u /home/mnt/gdrive-fuse
pkill -f gdrive-fuse
```

---

## CI / Release Builds

In GitHub Actions, pass the credentials as repository secrets:

```yaml
- name: Build release
  run: make build-release
  env:
    CLIENT_ID: ${{ secrets.CLIENT_ID }}
    CLIENT_SECRET: ${{ secrets.CLIENT_SECRET }}
```

Add `CLIENT_ID` and `CLIENT_SECRET` under **Repository → Settings → Secrets and variables → Actions**.

---

## Developer Workflow

After cloning, set up the full dev environment in two steps:

```bash
make build          # Debug build + generates build/compile_commands.json
make install-hooks  # Installs pre-commit (pre-commit + commit-msg stages)
```

### Code quality targets

| Command | What it does |
|---|---|
| `make format` | Auto-formats all `src/` and `include/` files with `clang-format` |
| `make lint` | Runs `clang-tidy` against `compile_commands.json` |
| `make lint-hooks` | Runs all pre-commit hooks on every file (`pre-commit run --all-files`) |

### Tooling files

| File | Purpose |
|---|---|
| [`.clang-format`](../.clang-format) | Google base style, 4-space indent, 100-char limit |
| [`.clang-tidy`](../.clang-tidy) | `cppcoreguidelines-*`, `modernize-*`, `bugprone-*`, naming conventions |
| [`.pre-commit-config.yaml`](../.pre-commit-config.yaml) | trailing-whitespace, end-of-file, clang-format, shellcheck, Conventional Commits |
| [`.githooks/`](../.githooks/) | Standalone fallback scripts (used by pre-commit as entry points) |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `libfuse3-dev: package not found` | fuse3 not in repos | Install `fuse3` (runtime) and `libfuse3-dev` (headers) separately |
| `CMake Error: OpenSSL not found` | Missing dev headers | `sudo apt-get install libssl-dev` |
| `fusermount3: Permission denied` | User not in fuse group | `sudo usermod -aG fuse $USER` and re-login |
| `getattr` floods log with API calls | Metadata cache miss | Expected on cold start; warms up after first `readdir` |
| `Only files with binary content can be downloaded` | Google Workspace file | These appear as `.desktop` shortcuts — open them to launch the browser |
| Token refresh fails after long idle | Refresh token expired | Delete `.gdrive_tokens.json` and re-authenticate |
