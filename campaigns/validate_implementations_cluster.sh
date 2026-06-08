#!/usr/bin/env bash
# Cluster-side determinism proof.
#
# Local `validate_implementations.sh` skips MPI because rsmpi 0.8 is
# incompatible with OpenMPI 5.x. This script ships the determinism test suite
# to compute6 (where OpenMPI 4.1.5 lives under /home/users/sconde/opt/) and
# runs it there, hitting the MPI variants for BFS, SSSP, PageRank, and LP.
#
# Burst + Spark cluster-determinism are *not* asserted here. This proof only
# covers standalone, Rayon, and MPI. In the 2026-05-29 campaign the raw timing
# records have validation.performed=false, so Burst/Spark correctness must not
# be inferred from this log.
#
# Usage:
#   bash campaigns/validate_implementations_cluster.sh
#
# Override the host/key/remote root via env vars (same conventions as
# launch_campaign_v3.sh):
#   CLOUDLAB_HOST=compute6  CLOUDLAB_SSH_KEY=~/.ssh/id_pc1
#   CLOUDLAB_SRC_ROOT=/home/users/sconde/src
#   MPI_PREFIX=/home/users/sconde/opt/openmpi-4.1.5

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

CLOUDLAB_HOST="${CLOUDLAB_HOST:-compute6}"
CLOUDLAB_SSH_KEY="${CLOUDLAB_SSH_KEY:-${HOME}/.ssh/id_pc1}"
CLOUDLAB_SRC_ROOT="${CLOUDLAB_SRC_ROOT:-/home/users/sconde/src}"
MPI_PREFIX="${MPI_PREFIX:-/home/users/sconde/opt/openmpi-4.1.5}"
SSH_OPTS=(-i "${CLOUDLAB_SSH_KEY}" -o StrictHostKeyChecking=accept-new)

echo "============================================================"
echo "  Cluster determinism proof — host: ${CLOUDLAB_HOST}"
echo "  Remote src root: ${CLOUDLAB_SRC_ROOT}"
echo "  OpenMPI prefix:  ${MPI_PREFIX}"
echo "============================================================"

# -- 1. SSH reachability --------------------------------------------------
if ! ssh "${SSH_OPTS[@]}" "${CLOUDLAB_HOST}" "true"; then
    echo "ERROR: cannot SSH to ${CLOUDLAB_HOST} with key ${CLOUDLAB_SSH_KEY}" >&2
    exit 1
fi

# -- 2. Sync test harness + modified Rust sources -------------------------
# Test harness:
rsync -az -e "ssh ${SSH_OPTS[*]}" \
    tests/determinism_fixtures.py \
    tests/determinism_compare.py \
    tests/test_cross_backend_correctness.py \
    tests/test_determinism_proof.py \
    "${CLOUDLAB_HOST}:${CLOUDLAB_SRC_ROOT}/tests/"

# Sync the *-rayon, *-standalone, *-mpi sources so the cluster has the
# updated mains that emit the full output vectors. We restrict the rsync
# pattern to source files only — never `target/` — so we don't ship local
# binaries that won't match the cluster's libc / OpenMPI ABI.
CRATES=(
    labelpropagation/lpst
    labelpropagation/lp-rayon
    labelpropagation/lp-mpi
    bfs/bfs-standalone
    bfs/bfs-rayon
    bfs/bfs-mpi
    sssp/sssp-standalone
    sssp/sssp-rayon
    sssp/sssp-mpi
    pagerank/pagerank-standalone
    pagerank/pagerank-rayon
    pagerank/pagerank-mpi
)
for crate in "${CRATES[@]}"; do
    src_dir="${crate}/src"
    if [[ -d "${src_dir}" ]]; then
        rsync -az -e "ssh ${SSH_OPTS[*]}" \
            "${src_dir}/" \
            "${CLOUDLAB_HOST}:${CLOUDLAB_SRC_ROOT}/${src_dir}/"
    fi
done

# -- 3. Build all backends on the cluster (idempotent) -------------------
echo "[build] Compiling all cost backends on ${CLOUDLAB_HOST}…"
ssh "${SSH_OPTS[@]}" "${CLOUDLAB_HOST}" bash -s <<EOF
set -e
export PATH="\$HOME/.cargo/bin:\$PATH"
export OPENMPI_DIR="${MPI_PREFIX}"
export PATH="\${OPENMPI_DIR}/bin:\$PATH"
export LD_LIBRARY_PATH="\${OPENMPI_DIR}/lib:\${LD_LIBRARY_PATH:-}"
# libclang (needed by mpi-sys/bindgen) lives in the python clang wheel on
# compute6 — same path used by the campaign compile scripts.
export LIBCLANG_PATH="\${LIBCLANG_PATH:-/home/users/sconde/.local/lib/python3.10/site-packages/clang/native}"
# The Python clang wheel ships libclang.so without the compiler's
# resource-dir headers (stddef.h, etc.). Point bindgen at the system gcc
# include dir so it can parse mpi.h.
GCC_INC=\$(ls -d /usr/lib/gcc/x86_64-linux-gnu/*/include 2>/dev/null | head -1)
export BINDGEN_EXTRA_CLANG_ARGS="\${BINDGEN_EXTRA_CLANG_ARGS:-} -I\${GCC_INC} -I/usr/include -I/usr/include/x86_64-linux-gnu"
cd "${CLOUDLAB_SRC_ROOT}"
for d in labelpropagation/lpst labelpropagation/lp-rayon labelpropagation/lp-mpi \
         bfs/bfs-standalone bfs/bfs-rayon bfs/bfs-mpi \
         sssp/sssp-standalone sssp/sssp-rayon sssp/sssp-mpi \
         pagerank/pagerank-standalone pagerank/pagerank-rayon pagerank/pagerank-mpi; do
    echo "  -> \${d}"
    # rsync -a preserves timestamps so cargo treats freshly synced sources
    # as up-to-date relative to target/. Touch every .rs to force rebuild.
    find "\${d}/src" -name "*.rs" -exec touch {} +
    (cd "\${d}" && cargo build --release --quiet)
done
EOF

# -- 4. Run the full determinism test suite on the cluster ----------------
echo
echo "[test] Running unittest tests.test_determinism_proof on ${CLOUDLAB_HOST}…"
ssh "${SSH_OPTS[@]}" "${CLOUDLAB_HOST}" bash -s <<EOF
set -e
export PATH="\$HOME/.cargo/bin:\$PATH"
export OPENMPI_DIR="${MPI_PREFIX}"
export PATH="\${OPENMPI_DIR}/bin:\$PATH"
export LD_LIBRARY_PATH="\${OPENMPI_DIR}/lib:\${LD_LIBRARY_PATH:-}"
# libclang (needed by mpi-sys/bindgen) lives in the python clang wheel on
# compute6 — same path used by the campaign compile scripts.
export LIBCLANG_PATH="\${LIBCLANG_PATH:-/home/users/sconde/.local/lib/python3.10/site-packages/clang/native}"
# The Python clang wheel ships libclang.so without the compiler's
# resource-dir headers (stddef.h, etc.). Point bindgen at the system gcc
# include dir so it can parse mpi.h.
GCC_INC=\$(ls -d /usr/lib/gcc/x86_64-linux-gnu/*/include 2>/dev/null | head -1)
export BINDGEN_EXTRA_CLANG_ARGS="\${BINDGEN_EXTRA_CLANG_ARGS:-} -I\${GCC_INC} -I/usr/include -I/usr/include/x86_64-linux-gnu"
cd "${CLOUDLAB_SRC_ROOT}"
python3 -m unittest tests.test_determinism_proof -v 2>&1
EOF

echo
echo "============================================================"
echo "  CLUSTER DETERMINISM PROOF PASSED"
echo "  - standalone, rayon, MPI agreement asserted on 5 fixtures"
echo "    for all 4 algos (LP, BFS, SSSP, PageRank)."
echo "  - Burst/Spark: NOT asserted by this log. In campaign 20260529,"
echo "    timing raw records have validation.performed=false; enable"
echo "    benchmark-level --validate in a future campaign if inline"
echo "    validation is required."
echo "============================================================"
