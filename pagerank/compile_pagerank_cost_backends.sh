#!/bin/bash
# Build PageRank COST backends (standalone + rayon + mpi).
# MPI backend requires rsmpi 0.8 + OpenMPI 4.x (CloudLab: 4.1.5). Skipped
# gracefully on hosts without OpenMPI dev headers.

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

build_crate "${SCRIPT_DIR}/pagerank-core"       "pagerank-core"
build_crate "${SCRIPT_DIR}/pagerank-standalone" "pagerank-standalone"
build_crate "${SCRIPT_DIR}/pagerank-rayon"      "pagerank-rayon"
build_crate "${SCRIPT_DIR}/pagerank-mpi"        "pagerank-mpi (requires OpenMPI 4.x)"

echo "==> Running pagerank-core tests"
if cargo test --manifest-path "${SCRIPT_DIR}/pagerank-core/Cargo.toml" --release; then
    echo "    OK: pagerank-core tests"
else
    echo "    FAIL: pagerank-core tests"
    RC=1
fi

exit "${RC}"
