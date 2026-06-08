#!/bin/bash
# Build BFS COST backends (standalone + rayon + mpi).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RC=0

build_crate() {
    local crate_dir="$1"
    local label="$2"
    if [ ! -f "${crate_dir}/Cargo.toml" ]; then
        echo "WARN: skip ${label}"
        return 0
    fi
    echo "==> Building ${label}"
    if cargo build --manifest-path "${crate_dir}/Cargo.toml" --release; then
        echo "    OK: ${label}"
    else
        echo "    FAIL: ${label}"
        RC=1
    fi
}

build_crate "${SCRIPT_DIR}/bfs-standalone" "bfs-standalone"
build_crate "${SCRIPT_DIR}/bfs-rayon"      "bfs-rayon"
build_crate "${SCRIPT_DIR}/bfs-mpi"        "bfs-mpi (requires OpenMPI 4.x)"

exit "${RC}"
