# ---------------------------------------------------------------------------
# gdrive-fuse – Project Makefile
# ---------------------------------------------------------------------------

# ── Configurable defaults ───────────────────────────────────────────────────
BUILD_DIR   ?= build
MOUNT_POINT ?= /home/mnt/gdrive-fuse
BINARY      := $(BUILD_DIR)/gdrive-fuse
NPROC       := $(shell nproc)

# All C++ source and header files (for format / lint targets)
SRCS := $(shell find src include -name '*.cpp' -o -name '*.hpp')

# OAuth2 credentials – override on the command line or via environment.
# In CI/CD the GitHub Action injects CLIENT_ID and CLIENT_SECRET as secrets.
CLIENT_ID     ?= $(error CLIENT_ID is not set – pass it on the command line or export it)
CLIENT_SECRET ?= $(error CLIENT_SECRET is not set – pass it on the command line or export it)

# ── Phony targets ───────────────────────────────────────────────────────────
.PHONY: help build build-release run stop format lint lint-hooks install-hooks

# ── help ────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  gdrive-fuse – available targets"
	@echo "  ────────────────────────────────────────────────────────────────"
	@echo "  help           Show this help message"
	@echo "  build          Configure (Debug) + compile into $(BUILD_DIR)/"
	@echo "  build-release  Configure (Release) + compile into $(BUILD_DIR)/"
	@echo "                 Requires CLIENT_ID and CLIENT_SECRET:"
	@echo "                   make build-release CLIENT_ID=... CLIENT_SECRET=..."
	@echo "                   or export CLIENT_ID / CLIENT_SECRET before calling make"
	@echo "  run            Mount Google Drive at $(MOUNT_POINT)"
	@echo "                 Requires CLIENT_ID and CLIENT_SECRET (see above)"
	@echo "                 Override mount point:  make run MOUNT_POINT=/your/path"
	@echo "  stop           Unmount and stop the running instance"
	@echo "  format         Auto-format all C++ files with clang-format"
	@echo "  lint           Run clang-tidy static analysis (requires compile_commands.json)"
	@echo "  lint-hooks     Run all pre-commit hooks on every file in the repo"
	@echo "  install-hooks  Install pre-commit hooks (pre-commit + commit-msg stages)"
	@echo "  ────────────────────────────────────────────────────────────────"
	@echo ""

# ── build (Debug) ───────────────────────────────────────────────────────────
build:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	cmake --build $(BUILD_DIR) --parallel $(NPROC)

# ── build-release ───────────────────────────────────────────────────────────
# Validates that credentials are present so the Release binary is never
# accidentally built without them being available at runtime.
build-release: _check-creds
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	cmake --build $(BUILD_DIR) --parallel $(NPROC)

# ── run ─────────────────────────────────────────────────────────────────────
run: _check-creds $(BINARY)
	@mkdir -p "$(MOUNT_POINT)"
	@if mountpoint -q "$(MOUNT_POINT)"; then \
		echo "Already mounted at $(MOUNT_POINT) – run 'make stop' first."; \
		exit 1; \
	fi
	$(BINARY) \
		--client-id    "$(CLIENT_ID)"     \
		--client-secret "$(CLIENT_SECRET)" \
		"$(MOUNT_POINT)" -f &
	@echo "Mounted at $(MOUNT_POINT)  (PID $$!)"

# ── stop ────────────────────────────────────────────────────────────────────
stop:
	@if mountpoint -q "$(MOUNT_POINT)"; then \
		fusermount3 -u "$(MOUNT_POINT)" && echo "Unmounted $(MOUNT_POINT)"; \
	else \
		echo "$(MOUNT_POINT) is not mounted."; \
	fi
	@pkill -f "$(BINARY)" 2>/dev/null && echo "Process stopped." || true

# ── internal: credential guard ──────────────────────────────────────────────
.PHONY: _check-creds
_check-creds:
	@test -n "$(CLIENT_ID)"     || (echo "ERROR: CLIENT_ID is not set";     exit 1)
	@test -n "$(CLIENT_SECRET)" || (echo "ERROR: CLIENT_SECRET is not set"; exit 1)

# ── format ──────────────────────────────────────────────────────────────────
format:
	@command -v clang-format &>/dev/null || \
		(echo "ERROR: clang-format not found. Install with: sudo apt-get install clang-format"; exit 1)
	clang-format --style=file -i $(SRCS)
	@echo "Formatting done."

# ── lint ────────────────────────────────────────────────────────────────────
lint: $(BUILD_DIR)/compile_commands.json
	@command -v clang-tidy &>/dev/null || \
		(echo "ERROR: clang-tidy not found. Install with: sudo apt-get install clang-tidy"; exit 1)
	clang-tidy -p $(BUILD_DIR) $(shell find src -name '*.cpp')

$(BUILD_DIR)/compile_commands.json:
	@echo "compile_commands.json not found – running 'make build' first."
	$(MAKE) build

# ── lint-hooks ──────────────────────────────────────────────────────────────
lint-hooks:
	@command -v pre-commit &>/dev/null || \
		(echo "ERROR: pre-commit not found. Install with: pip install pre-commit"; exit 1)
	pre-commit run --all-files

# ── install-hooks ───────────────────────────────────────────────────────────
# Installs pre-commit for the pre-commit and commit-msg stages.
# The scripts in .githooks/ remain as a manual fallback.
install-hooks:
	@command -v pre-commit &>/dev/null || \
		(echo "ERROR: pre-commit not found. Install with: pip install pre-commit"; exit 1)
	pre-commit install --hook-type pre-commit --hook-type commit-msg
	@echo "pre-commit hooks installed (pre-commit + commit-msg stages)."
