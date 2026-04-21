# ---------------------------------------------------------------------------
# gdrive-fuse – Top-Level Makefile (multi-language monorepo)
#
# Structure:
#   clients/cpp/   – C++20 implementation (CMake)
#   clients/rust/  – Rust implementation  (Cargo)
#
# Add further clients under clients/<lang>/ and extend the targets below.
# ---------------------------------------------------------------------------

# ── Configurable defaults ───────────────────────────────────────────────────
CPP_BUILD_DIR  ?= clients/cpp/build
CPP_SRC_DIR    := clients/cpp
CPP_BINARY     := $(CPP_BUILD_DIR)/gdrive-fuse

RUST_SRC_DIR   := clients/rust
RUST_BINARY    := $(RUST_SRC_DIR)/target/release/gdrive-fuse-rs
RUST_BINARY_DBG:= $(RUST_SRC_DIR)/target/debug/gdrive-fuse-rs

MOUNT_POINT    ?= $(HOME)/mnt/gdrive
NPROC          := $(shell nproc)

# ── Version (local builds) ──────────────────────────────────────────────────────
# CI sets GDRIVE_FUSE_VERSION externally; for local builds it is derived from
# 'git describe'. Format examples:
#   tag only:          1.2.3
#   tag + dirty:       1.2.3-dirty
#   commits ahead:     1.2.3-loc-42
#   ahead + dirty:     1.2.3-loc-42-dirty
ifeq ($(origin GDRIVE_FUSE_VERSION),undefined)
  _GIT_DESCRIBE := $(shell git describe --tags --dirty --abbrev=7 2>/dev/null)
  ifneq ($(_GIT_DESCRIBE),)
    GDRIVE_FUSE_VERSION := $(shell printf '%s' '$(_GIT_DESCRIBE)' | \
      sed -e 's|^v||' \
          -e 's|\([0-9]*\.[0-9]*\.[0-9]*\)-\([0-9]*\)-g[0-9a-f]*|\1-loc-\2|')
  endif
endif
export GDRIVE_FUSE_VERSION

# C++ source files for format / lint
SRCS := $(shell find clients/cpp/src clients/cpp/include -name '*.cpp' -o -name '*.hpp' 2>/dev/null)

# Legacy alias kept for existing scripts
BUILD_DIR  := $(CPP_BUILD_DIR)
BINARY     := $(CPP_BINARY)

# OAuth2 credentials – resolved in priority order:
#   1. Command-line argument or environment variable (CLIENT_ID / CLIENT_SECRET)
#   2. credentials.json (if present – requires jq)
# The _check-creds guard verifies they are non-empty before any mount/build.
CREDENTIALS_JSON ?= credentials.json
CLIENT_ID        ?= $(shell test -f "$(CREDENTIALS_JSON)" && jq -r '.installed.client_id'     "$(CREDENTIALS_JSON)" 2>/dev/null)
CLIENT_SECRET    ?= $(shell test -f "$(CREDENTIALS_JSON)" && jq -r '.installed.client_secret' "$(CREDENTIALS_JSON)" 2>/dev/null)

# ── Phony targets ───────────────────────────────────────────────────────────
.PHONY: help \
        build build-cpp build-rust \
        build-release build-cpp-release build-rust-release \
        run run-cpp run-rust run-rust-dbg \
        stop stop-cpp stop-rust \
        format format-cpp format-rust \
        lint lint-cpp lint-rust \
        lint-hooks install-hooks bench

# ── help ────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  gdrive-fuse – available targets"
	@echo "  ────────────────────────────────────────────────────────────────────"
	@echo "  help               Show this help"
	@echo ""
	@echo "  Build (Debug)"
	@echo "  build              Build all clients"
	@echo "  build-cpp          Build C++ client"
	@echo "  build-rust         Build Rust client"
	@echo ""
	@echo "  Build (Release)"
	@echo "  build-release      Build all clients (Release)"
	@echo "  build-cpp-release  Build C++ client (Release)"
	@echo "  build-rust-release Build Rust client (Release)"
	@echo ""
	@echo "  Mount / Unmount"
	@echo "  run-cpp            Mount with C++ client at \$(MOUNT_POINT)"
	@echo "  run-rust           Mount with Rust client at \$(MOUNT_POINT) (background)"
	@echo "  run-rust-dbg       Mount with Rust client, foreground log (CTRL-C to stop)"
	@echo "  stop               Unmount \$(MOUNT_POINT)"
	@echo ""
	@echo "  Code Quality"
	@echo "  format             Format all clients"
	@echo "  format-cpp         clang-format on C++ sources"
	@echo "  format-rust        cargo fmt on Rust sources"
	@echo "  lint               Lint all clients"
	@echo "  lint-cpp           clang-tidy"
	@echo "  lint-rust          cargo clippy"
	@echo "  lint-hooks         Run all pre-commit hooks"
	@echo "  install-hooks      Install pre-commit hooks"
	@echo ""
	@echo "  bench              Performance comparison (requires hyperfine)"
	@echo ""
	@echo "  Options: CLIENT_ID=... CLIENT_SECRET=... MOUNT_POINT=... CREDENTIALS_JSON=..."
	@echo "  Credentials are auto-read from credentials.json (requires jq) if not set."
	@echo "  ────────────────────────────────────────────────────────────────────"
	@echo ""

# ── build (Debug) ───────────────────────────────────────────────────────────
build: build-cpp build-rust

build-cpp:
	cmake -S $(CPP_SRC_DIR) -B $(CPP_BUILD_DIR) \
	      -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	      $(if $(GDRIVE_FUSE_VERSION),-DGDRIVE_FUSE_VERSION="$(GDRIVE_FUSE_VERSION)")
	cmake --build $(CPP_BUILD_DIR) --parallel $(NPROC)

build-rust:
	cd $(RUST_SRC_DIR) && cargo build

# ── build (Release) ─────────────────────────────────────────────────────────
build-release: build-cpp-release build-rust-release

build-cpp-release: _check-creds
	cmake -S $(CPP_SRC_DIR) -B $(CPP_BUILD_DIR) \
	      -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	      $(if $(GDRIVE_FUSE_VERSION),-DGDRIVE_FUSE_VERSION="$(GDRIVE_FUSE_VERSION)")
	cmake --build $(CPP_BUILD_DIR) --parallel $(NPROC)

build-rust-release: _check-creds
	cd $(RUST_SRC_DIR) && cargo build --release

# ── run ─────────────────────────────────────────────────────────────────────
run-cpp: _check-creds $(CPP_BINARY)
	@mkdir -p "$(MOUNT_POINT)"
	@if mountpoint -q "$(MOUNT_POINT)"; then \
		echo "Already mounted – run 'make stop' first."; exit 1; \
	fi
	$(CPP_BINARY) \
		--client-id    "$(CLIENT_ID)"     \
		--client-secret "$(CLIENT_SECRET)" \
		"$(MOUNT_POINT)" -f &
	@echo "[cpp]  Mounted at $(MOUNT_POINT) (PID $$!)"

run-rust: _check-creds $(RUST_BINARY_DBG)
	@mkdir -p "$(MOUNT_POINT)"
	@if mountpoint -q "$(MOUNT_POINT)"; then \
		echo "Already mounted – run 'make stop' first."; exit 1; \
	fi
	$(RUST_BINARY_DBG) \
		--client-id    "$(CLIENT_ID)"     \
		--client-secret "$(CLIENT_SECRET)" \
		"$(MOUNT_POINT)" &
	@echo "[rust] Mounted at $(MOUNT_POINT) (PID $$!)"

run-rust-dbg: _check-creds $(RUST_BINARY_DBG)
	@mkdir -p "$(MOUNT_POINT)"
	@if grep -qs " $(MOUNT_POINT) " /proc/mounts; then \
		echo "Already mounted – run 'make stop' first."; exit 1; \
	fi
	@echo "[rust] Mounting at $(MOUNT_POINT) – press CTRL-C to unmount and exit"
	RUST_LOG=$${RUST_LOG:-info} $(RUST_BINARY_DBG) \
		--client-id    "$(CLIENT_ID)"     \
		--client-secret "$(CLIENT_SECRET)" \
		"$(MOUNT_POINT)"

# ── stop ────────────────────────────────────────────────────────────────────
# Kill both FUSE daemons first, then unmount.  Use /proc/mounts instead of
# `mountpoint -q` because `mountpoint -q` returns non-zero for stale FUSE
# mounts (daemon gone, kernel entry still present).
stop: stop-cpp stop-rust
	@if grep -qs " $(MOUNT_POINT) " /proc/mounts; then \
		fusermount3 -u "$(MOUNT_POINT)" && echo "Unmounted $(MOUNT_POINT)"; \
	else \
		echo "$(MOUNT_POINT) is not mounted."; \
	fi

stop-cpp:
	@pkill -x gdrive-fuse 2>/dev/null && echo "[cpp] Process stopped." || true

stop-rust:
	@pkill -x gdrive-fuse-rs 2>/dev/null && echo "[rust] Process stopped." || true

# ── internal: credential guard ──────────────────────────────────────────────
.PHONY: _check-creds
_check-creds:
	@test -n "$(CLIENT_ID)"     || (echo "ERROR: CLIENT_ID is not set and could not be read from $(CREDENTIALS_JSON) (is jq installed?)";     exit 1)
	@test -n "$(CLIENT_SECRET)" || (echo "ERROR: CLIENT_SECRET is not set and could not be read from $(CREDENTIALS_JSON) (is jq installed?)"; exit 1)

# ── format ──────────────────────────────────────────────────────────────────
format: format-cpp format-rust

format-cpp:
	@command -v clang-format &>/dev/null || \
		(echo "ERROR: clang-format not found. Install with: sudo apt-get install clang-format"; exit 1)
	clang-format --style=file -i $(SRCS)
	@echo "[cpp] Formatting done."

format-rust:
	cd $(RUST_SRC_DIR) && cargo fmt
	@echo "[rust] Formatting done."

# ── lint ────────────────────────────────────────────────────────────────────
lint: lint-cpp lint-rust

lint-cpp: $(CPP_BUILD_DIR)/compile_commands.json
	@command -v clang-tidy &>/dev/null || \
		(echo "ERROR: clang-tidy not found. Install with: sudo apt-get install clang-tidy"; exit 1)
	clang-tidy -p $(CPP_BUILD_DIR) $(shell find clients/cpp/src -name '*.cpp')

$(CPP_BUILD_DIR)/compile_commands.json:
	@echo "compile_commands.json not found – running 'make build-cpp' first."
	$(MAKE) build-cpp

lint-rust:
	cd $(RUST_SRC_DIR) && cargo clippy -- -D warnings

# ── lint-hooks ──────────────────────────────────────────────────────────────
lint-hooks:
	@command -v pre-commit &>/dev/null || \
		(echo "ERROR: pre-commit not found. Install with: pip install pre-commit"; exit 1)
	pre-commit run --all-files

# ── install-hooks ───────────────────────────────────────────────────────────
install-hooks:
	@command -v pre-commit &>/dev/null || \
		(echo "ERROR: pre-commit not found. Install: pip install pre-commit"; exit 1)
	pre-commit install --hook-type pre-commit --hook-type commit-msg
	@echo "pre-commit hooks installed (pre-commit + commit-msg stages)."

# ── bench ───────────────────────────────────────────────────────────────────
bench: _check-creds
	@command -v hyperfine &>/dev/null || \
		(echo "ERROR: hyperfine not found. Install: cargo install hyperfine"; exit 1)
	./benchmarks/run.sh "$(CLIENT_ID)" "$(CLIENT_SECRET)"
