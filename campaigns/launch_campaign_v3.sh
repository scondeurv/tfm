#!/usr/bin/env bash
# Launch the v3 unified TFM campaign on CloudLab.
#
# Reproduces campaign-unified-20260524T064518Z's matrix:
#   - 4 algorithms: lp, bfs, sssp, pagerank
#   - 5 backends: standalone, rayon, mpi, burst, spark
#   - sizes n=10k, 100k, 1M, 10M (COST: full sweep)
#   - burst partitions p=4, p=8; granularity/memory selected by config_sweep
#   - rayon threads 1, 4, 16, 32
#   - mpi ranks 4, 16, 32 over compute6 + compute7 (1 GbE), mapped by node
#   - 3 reps per cell for size sweep and COST
#
# Prerequisites (NOT done by this script — operator's responsibility):
#
#   1. CloudLab nodes compute2, compute4, compute5, compute6, compute7
#      provisioned with Ubuntu 22.04 + K8s 1.28.
#   2. OpenWhisk Burst deployed via openwhisk-deploy-kube-burst/ with
#      BURST_STANDALONE=true on both controller and invoker, and
#      `kubectl -n openwhisk set env statefulset/owdev-invoker
#      CONFIG_whisk_containerPool_userMemory=65536m` applied so that
#      8 × 4 GB workers fit in a single invoker.
#   3. OpenMPI 4.1.5 built user-space on compute6 at
#      /home/users/sconde/opt/openmpi-4.1.5 (rsmpi 0.8 incompatible with
#      OpenMPI 5.x). Rust toolchain via rustup. LIBCLANG_PATH set in the
#      remote shell for bindgen.
#   4. Cost-backend binaries compiled on compute6 under
#      /home/users/sconde/src/{labelpropagation,bfs,sssp,pagerank}/<crate>/
#      target/release/. Run the compile_*_cost_backends.sh scripts in each
#      algo directory once.
#   5. Burst action zips compiled and present locally under
#      <algo>/<algo>.zip (run compile_*_cluster.sh).
#   6. MinIO running with bucket `tfm-smoke` and credentials lab144 /
#      astl1a4b4 in /home/sergio/src/.env (sourced below).
#   7. K8s spark cluster manifests staged under baselines/cloudlab/k8s/.
#      The Spark cluster itself comes up as part of each Spark cell's
#      smoke wrapper, so no manual deploy is required.
#
# Override any matrix dimension via the corresponding env var below.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# -- Credentials (MinIO inside CloudLab) --------------------------------
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY required (put them in .env)" >&2
  exit 1
fi

# -- Campaign identity --------------------------------------------------
TS="$(date -u +%Y%m%dT%H%M%SZ)"
CAMPAIGN_ROOT="${CAMPAIGN_ROOT:-experiment_data/cloudlab_campaigns/campaign-unified-${TS}}"

# -- Matrix parameters (defaults match the canonical v3 campaign) -------
BACKENDS="${BACKENDS:-standalone,rayon,mpi,burst,spark}"
SIZE_NODES="${SIZE_NODES:-100000,1000000,10000000}"
COST_SWEEP_NODES="${COST_SWEEP_NODES:-10000,100000,1000000,10000000}"
BURST_PARTITIONS="${BURST_PARTITIONS:-4,8}"
# Warm-pool protocol: 1 cold descartada + N warm reps per Burst cell. Mediana
# warm reemplaza el mean_end_to_end_ms. 0 = legacy single-shot. Default 9
# (~3-4h size_sweep). Reducir a 5 si SSSP/PR n=10M sufre eviction intra-cell.
BURST_WARMUP_SHOTS="${BURST_WARMUP_SHOTS:-9}"
# Per-n warm-pool shots override. Default caps n=10M at 5 (eviction risk: 9
# warm shots at n=10M for SSSP/PR can exceed OW idleTimeout ~10min).
# Format: 'n:shots[,n:shots,...]'.
BURST_WARMUP_SHOTS_OVERRIDES="${BURST_WARMUP_SHOTS_OVERRIDES:-10000000:5}"
SPARK_PARTITIONS="${SPARK_PARTITIONS:-4}"
SPARK_EXECUTORS="${SPARK_EXECUTORS:-4}"
SPARK_CONFIG_MEMORIES="${SPARK_CONFIG_MEMORIES:-4g}"
SPARK_CELL_TIMEOUT_SEC="${SPARK_CELL_TIMEOUT_SEC:-5400}"
SPARK_KUBECTL_HOST="${SPARK_KUBECTL_HOST:-cloudfunctions.urv.cat}"
# Spark size tiers (M4 Nivel 2, decisión 2026-05-31):
#   n ∈ {100k, 500k, 1M, 2M} → default cap 90 min (5400s) + executor 4g.
#   n = 5M → extended tier with hard timeout 15 min (900s) + executor 12g.
#     If GraphX converges → datapoint in canonical table. If not → raw_run
#     records status=timeout_900s as a measured structural finding.
#   n = 10M excluido: GraphX cross-host 1GbE no converge en ningún budget
#     razonable y executor mem requerido >16Gi. Burst + COST extienden hasta
#     n=10M sin Spark.
SPARK_SIZE_NODES="${SPARK_SIZE_NODES:-100000,500000,1000000,2000000,5000000}"
SPARK_SIZE_TIMEOUT_OVERRIDES="${SPARK_SIZE_TIMEOUT_OVERRIDES:-5000000:900}"
SPARK_EXECUTOR_MEMORY_OVERRIDES="${SPARK_EXECUTOR_MEMORY_OVERRIDES:-5000000:12g}"
RAYON_THREADS="${RAYON_THREADS:-1,4,16,32}"
MPI_RANKS="${MPI_RANKS:-4,8,16,32}"
MPI_HOSTS="${MPI_HOSTS:-compute6:32,compute7:32}"
MPI_MAP_BY="${MPI_MAP_BY-node}"
MPI_BTL_IF_INCLUDE="${MPI_BTL_IF_INCLUDE-192.168.5.0/24}"
MPI_PREFIX="${MPI_PREFIX:-/home/users/sconde/opt/openmpi-4.1.5}"
CONFIG_RUNS="${CONFIG_RUNS:-1}"
SIZE_RUNS="${SIZE_RUNS:-3}"
COST_RUNS="${COST_RUNS:-3}"
# Per-n COST repetitions override. Default bumps n=10M to 5 reps to tighten
# the tail CV (was ~6% at SIZE_RUNS=3). Format: 'n:reps[,n:reps,...]'.
COST_RUNS_OVERRIDES="${COST_RUNS_OVERRIDES:-10000000:5}"
MAX_ITER="${MAX_ITER:-20}"
BURST_REMOTE_TIMEOUT_SEC="${BURST_REMOTE_TIMEOUT_SEC:-1800}"
BURST_EFFECTIVE_INVOKERS="${BURST_EFFECTIVE_INVOKERS:-1}"
BURST_EFFECTIVE_USER_MEMORY_MB="${BURST_EFFECTIVE_USER_MEMORY_MB:-65536}"

# -- CloudLab + OW endpoints --------------------------------------------
CLOUDLAB_HOST="${CLOUDLAB_HOST:-compute6}"
CLOUDLAB_SSH_KEY="${CLOUDLAB_SSH_KEY:-${HOME}/.ssh/id_pc1}"
CLOUDLAB_SSH_CONFIG="${CLOUDLAB_SSH_CONFIG:-}"
if [[ -z "${CLOUDLAB_SSH_CONFIG}" && -f "${HOME}/.ssh/config" ]]; then
  CLOUDLAB_SSH_CONFIG="${HOME}/.ssh/config"
fi
CLOUDLAB_SRC_ROOT="${CLOUDLAB_SRC_ROOT:-/home/users/sconde/src}"
OW_HOST="${OW_HOST:-10.99.125.88}"
OW_PORT="${OW_PORT:-80}"
OW_NAMESPACE="${OW_NAMESPACE:-openwhisk}"
OW_RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"

# -- Run --------------------------------------------------------------
mkdir -p "${CAMPAIGN_ROOT}/logs"

ALGORITHMS="${ALGORITHMS:-lp bfs sssp pagerank}"
PHASE="${PHASE:-full}"

COMMON_ARGS=(
  --campaign-root "${CAMPAIGN_ROOT}"
  --backends "${BACKENDS}"
  --size-nodes "${SIZE_NODES}"
  --cost-sweep-nodes "${COST_SWEEP_NODES}"
  --burst-partitions "${BURST_PARTITIONS}"
  --spark-partitions "${SPARK_PARTITIONS}"
  --spark-total-executors "${SPARK_EXECUTORS}"
  --spark-config-memories "${SPARK_CONFIG_MEMORIES}"
  --spark-cell-timeout-sec "${SPARK_CELL_TIMEOUT_SEC}"
  --spark-kubectl-host "${SPARK_KUBECTL_HOST}"
  --spark-size-nodes "${SPARK_SIZE_NODES}"
  --spark-size-timeout-overrides "${SPARK_SIZE_TIMEOUT_OVERRIDES}"
  --spark-executor-memory-overrides "${SPARK_EXECUTOR_MEMORY_OVERRIDES}"
  --rayon-threads "${RAYON_THREADS}"
  --mpi-ranks "${MPI_RANKS}"
  --mpi-hosts "${MPI_HOSTS}"
  --mpi-map-by "${MPI_MAP_BY}"
  --mpi-btl-if-include "${MPI_BTL_IF_INCLUDE}"
  --mpi-prefix "${MPI_PREFIX}"
  --config-runs "${CONFIG_RUNS}"
  --size-runs "${SIZE_RUNS}"
  --cost-runs "${COST_RUNS}"
  --cost-runs-overrides "${COST_RUNS_OVERRIDES}"
  --max-iter "${MAX_ITER}"
  --burst-remote-timeout-sec "${BURST_REMOTE_TIMEOUT_SEC}"
  --burst-warmup-shots "${BURST_WARMUP_SHOTS}"
  --burst-warmup-shots-overrides "${BURST_WARMUP_SHOTS_OVERRIDES}"
  --burst-effective-invokers "${BURST_EFFECTIVE_INVOKERS}"
  --burst-effective-user-memory-mb "${BURST_EFFECTIVE_USER_MEMORY_MB}"
  --cloudlab-host "${CLOUDLAB_HOST}"
  --cloudlab-ssh-key "${CLOUDLAB_SSH_KEY}"
  --cloudlab-src-root "${CLOUDLAB_SRC_ROOT}"
  --ow-host "${OW_HOST}"
  --ow-port "${OW_PORT}"
  --ow-namespace "${OW_NAMESPACE}"
  --ow-release-name "${OW_RELEASE_NAME}"
)

if [[ -n "${CLOUDLAB_SSH_CONFIG}" ]]; then
  COMMON_ARGS+=(--cloudlab-ssh-config "${CLOUDLAB_SSH_CONFIG}")
fi

# Pre-stub best_config_<algo>_pN.json so the size phase doesn't depend on a
# previous config sweep run. This now seeds only the GRANULARITY winner; the
# size_sweep memory limit is computed per-n by burst_memory_mb() in
# cloudlab_common.py (scaled with graph size + clamped to the user-memory
# budget, and fit-checked against the node cap), which replaced the old flat
# m=4096 knob that OOM'd at n=10M. The memory_mb below remains only as a
# fallback for non-size phases (chunk_probe) that still read it. Namespaced
# per-algo (M2): avoids the trap where the last algo overwrites a previous
# algo's winners — bit us in campaign 0529 when re-running BFS read PR's
# config.
mkdir -p "${CAMPAIGN_ROOT}/config_sweep"
for algo in ${ALGORITHMS}; do
  for bp in $(echo "${BURST_PARTITIONS}" | tr ',' ' '); do
    CFG="${CAMPAIGN_ROOT}/config_sweep/best_config_${algo}_p${bp}.json"
    if [[ ! -f "${CFG}" ]]; then
      cat > "${CFG}" <<EOF
{"burst":{"granularity":1,"memory_mb":4096}}
EOF
    fi
  done
done

for algo in ${ALGORITHMS}; do
  echo "============================================================"
  echo "[$(date -u +%H:%M:%S)] Algorithm: ${algo}  (campaign-unified-${TS})"
  echo "============================================================"
  python3 campaigns/run_cloudlab_campaign.py \
    --algorithm "${algo}" \
    --phase "${PHASE}" \
    "${COMMON_ARGS[@]}" \
    2>&1 | tee "${CAMPAIGN_ROOT}/logs/${algo}_campaign.log"
  echo ""
done

echo "============================================================"
echo "CAMPAIGN COMPLETE"
echo "  Root:    ${CAMPAIGN_ROOT}"
echo "  Tables:  ${CAMPAIGN_ROOT}/report/<algo>/{cross_backend,cost,size_burst}_table.md"
echo "  Summary: ${CAMPAIGN_ROOT}/report/summary.md"
echo "============================================================"
