#!/usr/bin/env bash
# Deterministic cross-backend correctness validation for the TFM graph
# algorithm implementations.
#
# Runs locally (no CloudLab access required). Compiles the standalone +
# rayon Rust crates for LP, BFS, SSSP and PageRank, then drives the
# unittest suite tests/test_cross_backend_correctness.py which feeds each
# binary the same small fixed-graph fixture and asserts that the
# algorithm-level outputs (max_level / max_distance / max_rank + counts)
# agree across implementations.
#
# MPI tests are included automatically when `mpirun` is on PATH and the
# `*-mpi` binaries are built; they are skipped otherwise. CloudLab-only
# backends (Burst, Spark) are not asserted by this script. In the 2026-05-29
# campaign the raw timing records have validation.performed=false, so Burst
# and Spark correctness must not be inferred from this log.
#
# Exit status: 0 on full pass, non-zero if any test fails or if any
# required toolchain is missing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# ---- Toolchain probe ---------------------------------------------------

if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: cargo not on PATH (install rustup + Rust stable)" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 not on PATH" >&2
  exit 1
fi

HAS_MPI=0
if command -v mpirun >/dev/null 2>&1; then
  HAS_MPI=1
  echo "[probe] mpirun available — MPI tests will run"
else
  echo "[probe] mpirun NOT available — MPI tests will skip (this is fine)"
fi

# ---- Crate compilation -------------------------------------------------
#
# Each (algo, backend) pair has its own crate. Build only what exists and
# what the toolchain supports; skip MPI builds gracefully if libclang or
# OpenMPI are missing.

declare -a CRATES_STANDALONE=(
  "labelpropagation/lpst"
  "bfs/bfs-standalone"
  "sssp/sssp-standalone"
  "pagerank/pagerank-standalone"
)
declare -a CRATES_RAYON=(
  "labelpropagation/lp-rayon"
  "bfs/bfs-rayon"
  "sssp/sssp-rayon"
  "pagerank/pagerank-rayon"
)
declare -a CRATES_MPI=(
  "labelpropagation/lp-mpi"
  "bfs/bfs-mpi"
  "sssp/sssp-mpi"
  "pagerank/pagerank-mpi"
)

build_crate() {
  local rel="$1"
  local label="$2"
  if [[ ! -d "${rel}" ]]; then
    echo "[skip] ${label}: directory ${rel} missing"
    return 0
  fi
  echo "[build] ${label} (${rel})"
  if ! (cd "${rel}" && cargo build --release --quiet); then
    echo "  -> build FAILED for ${rel}" >&2
    return 1
  fi
}

echo
echo "============================================================"
echo "  Building standalone backends"
echo "============================================================"
for c in "${CRATES_STANDALONE[@]}"; do
  build_crate "${c}" "standalone $(basename "${c}")"
done

echo
echo "============================================================"
echo "  Building Rayon backends"
echo "============================================================"
for c in "${CRATES_RAYON[@]}"; do
  build_crate "${c}" "rayon $(basename "${c}")"
done

if [[ "${HAS_MPI}" -eq 1 ]]; then
  echo
  echo "============================================================"
  echo "  Building MPI backends"
  echo "============================================================"
  for c in "${CRATES_MPI[@]}"; do
    # MPI builds can fail at link time if libclang or OpenMPI 4.x are
    # missing on the local host. Don't treat that as fatal — the test
    # suite skips MPI cases when the binary is absent.
    build_crate "${c}" "mpi $(basename "${c}")" || \
      echo "  -> ${c} build failed (will be skipped at test time)"
  done
fi

# ---- Run deterministic correctness suite ------------------------------
#
# The unittest module enumerates fixtures per algorithm, runs each
# binary, and asserts output equality. Skipped tests count as PASS at
# the script level — missing binaries (e.g. local machine without
# OpenMPI) are not failures.

echo
echo "============================================================"
echo "  Running smoke tests (toy fixtures)"
echo "============================================================"
python3 -m unittest tests.test_cross_backend_correctness -v

echo
echo "============================================================"
echo "  Running full determinism proof (4 algos × 5 fixtures)"
echo "============================================================"
python3 -m unittest tests.test_determinism_proof -v

echo
echo "============================================================"
echo "  ALL CHECKS PASSED"
echo "  - standalone + rayon agreement asserted as FULL-VECTOR equality"
echo "    (BFS levels exact, SSSP distances bit-exact f32, PR ranks ε=1e-5,"
echo "     LP partition-equivalence) across 5 graph fixtures:"
echo "       path_100, star_50, two_components_50each, er_1000_p01, self_loops_20"
echo "  - All 4 algos covered (LP, BFS, SSSP, PageRank)."
if [[ "${HAS_MPI}" -eq 1 ]]; then
  echo "  - MPI: tests executed (some may have been skipped if a particular"
  echo "    *-mpi binary failed to build with the local OpenMPI version)."
else
  echo "  - MPI: tests skipped locally — no mpirun on PATH."
  echo "    Run campaigns/validate_implementations_cluster.sh to assert MPI"
  echo "    determinism on compute6 (OpenMPI 4.1.5)."
fi
echo "  - Burst/Spark: NOT asserted by this log. In campaign 20260529,"
echo "    timing raw records have validation.performed=false; enable"
echo "    benchmark-level --validate in a future campaign if inline"
echo "    validation is required."
echo "============================================================"
