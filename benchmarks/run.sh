#!/usr/bin/env bash
# benchmarks/run.sh
# ──────────────────────────────────────────────────────────────────────────
# Performance comparison between the C++ and Rust gdrive-fuse clients.
#
# Prerequisites:
#   - hyperfine  (cargo install hyperfine)
#   - Both Release binaries built:
#       make build-cpp-release build-rust-release CLIENT_ID=... CLIENT_SECRET=...
#
# Usage:
#   ./benchmarks/run.sh <CLIENT_ID> <CLIENT_SECRET> [MOUNT_POINT]
#
# The script mounts each client in turn, runs the benchmark suite, then
# unmounts and prints a comparison table.
# ──────────────────────────────────────────────────────────────────────────

set -euo pipefail

CLIENT_ID="${1:?Usage: $0 <CLIENT_ID> <CLIENT_SECRET> [MOUNT_POINT]}"
CLIENT_SECRET="${2:?}"
MOUNT_POINT="${3:-/tmp/gdrive-bench}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CPP_BIN="${REPO_ROOT}/clients/cpp/build/gdrive-fuse"
RUST_BIN="${REPO_ROOT}/clients/rust/target/release/gdrive-fuse-rs"
RESULTS_DIR="${REPO_ROOT}/benchmarks/results"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

mkdir -p "${RESULTS_DIR}" "${MOUNT_POINT}"

# ── helpers ────────────────────────────────────────────────────────────────

mount_client() {
    local binary="$1"
    local label="$2"
    echo "[bench] Mounting ${label} at ${MOUNT_POINT} …"
    "${binary}" \
        --client-id     "${CLIENT_ID}" \
        --client-secret "${CLIENT_SECRET}" \
        "${MOUNT_POINT}" &
    sleep 2   # wait for FUSE to become ready
    mountpoint -q "${MOUNT_POINT}" || { echo "ERROR: mount failed for ${label}"; exit 1; }
    echo "[bench] ${label} mounted"
}

unmount_client() {
    if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
        fusermount3 -u "${MOUNT_POINT}"
        echo "[bench] Unmounted ${MOUNT_POINT}"
    fi
    pkill -f "gdrive-fuse" 2>/dev/null || true
    sleep 1
}

run_suite() {
    local label="$1"
    local json_out="${RESULTS_DIR}/${TIMESTAMP}_${label}.json"

    echo ""
    echo "══════════════════════════════════════════════════════════════════"
    echo "  Benchmark suite: ${label}"
    echo "══════════════════════════════════════════════════════════════════"

    hyperfine \
        --warmup 1 \
        --runs 5 \
        --export-json "${json_out}" \
        --command-name "${label}: ls root" \
            "ls ${MOUNT_POINT} > /dev/null" \
        --command-name "${label}: stat each file" \
            "find ${MOUNT_POINT} -maxdepth 1 -exec stat {} + > /dev/null" \
        --command-name "${label}: find recursive (depth 3)" \
            "find ${MOUNT_POINT} -maxdepth 3 > /dev/null"

    echo "[bench] Results saved to ${json_out}"
}

# ── main ───────────────────────────────────────────────────────────────────

echo ""
echo "gdrive-fuse benchmark — ${TIMESTAMP}"
echo "Mount point: ${MOUNT_POINT}"
echo ""

# Validate binaries
for bin in "${CPP_BIN}" "${RUST_BIN}"; do
    if [[ ! -x "${bin}" ]]; then
        echo "ERROR: binary not found or not executable: ${bin}"
        echo "Run: make build-release CLIENT_ID=\$CLIENT_ID CLIENT_SECRET=\$CLIENT_SECRET"
        exit 1
    fi
done

trap 'unmount_client' EXIT

# ── C++ client ────────────────────────────────────────────────────────────
mount_client "${CPP_BIN}" "cpp"
run_suite "cpp"
unmount_client

# ── Rust client ───────────────────────────────────────────────────────────
mount_client "${RUST_BIN}" "rust"
run_suite "rust"
unmount_client

# ── Comparison ────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "  Results stored in: ${RESULTS_DIR}/"
echo "  Compare with:  hyperfine --load ${RESULTS_DIR}/${TIMESTAMP}_cpp.json"
echo "                            ${RESULTS_DIR}/${TIMESTAMP}_rust.json"
echo "══════════════════════════════════════════════════════════════════"
echo ""
