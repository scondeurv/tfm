#!/bin/bash
# Build LP COST backends (standalone + rayon + mpi) with cargo on the current
# host. Targets the CloudLab compute nodes. Run after `provision_mpi.sh` has
# installed OpenMPI on the node.
#
# Each crate compiles independently; failure in one (e.g. lp-mpi without
# OpenMPI 4.x) is reported but does not block the others.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RC=0

build_crate() {
    local crate_dir="$1"
    local label="$2"
    if [ ! -f "${crate_dir}/Cargo.toml" ]; then
        echo "WARN: skip ${label} (no Cargo.toml at ${crate_dir})"
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

build_crate "${SCRIPT_DIR}/lpst"      "lpst (standalone)"
build_crate "${SCRIPT_DIR}/lp-rayon"  "lp-rayon"
build_crate "${SCRIPT_DIR}/lp-mpi"    "lp-mpi (requires OpenMPI 4.x)"

if [ "${RC}" -ne 0 ]; then
    echo "One or more LP COST backends failed to build" >&2
fi
exit "${RC}"
