#!/usr/bin/env bash
# Full multi-algorithm CloudLab campaign launcher
# Run from: local machine with SSH access to CloudLab
# Prerequisites: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY set for MinIO
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# ---------------------------------------------------------------------------
# Validate environment
# ---------------------------------------------------------------------------
if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    echo "ERROR: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for CloudLab MinIO" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Common parameters
# ---------------------------------------------------------------------------
BURST_PARTITIONS="4,8,16"
SPARK_PARTITIONS="8"
SPARK_EXECUTORS="8"
SPARK_CONFIG_MEMORIES="4g,6g"
SIZE_NODES="100000,500000,1000000,2000000"
CONFIG_RUNS=3
SIZE_RUNS=3

# OpenWhisk deployment (verify namespace before running)
OW_NAMESPACE="${OW_NAMESPACE:-openwhisk}"
OW_RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"
OW_PORT="${OW_PORT:-31002}"

# Common OW flags (append to every runner invocation)
OW_FLAGS=(
    --ow-namespace "${OW_NAMESPACE}"
    --ow-release-name "${OW_RELEASE_NAME}"
    --ow-port "${OW_PORT}"
)

# ---------------------------------------------------------------------------
# Phase 1: LP — re-run chunk probe only (existing campaign, bug-fixed)
# All other phases hit cache (renamed raw files with _ck1024 suffix).
# ---------------------------------------------------------------------------
LP_CAMPAIGN_ROOT="experiment_data/cloudlab_campaigns/campaign-lp-20260423T125204Z"

echo "============================================================"
echo "Phase 1: LP — chunk probe (re-run with fixed caching)"
echo "  Campaign root: ${LP_CAMPAIGN_ROOT}"
echo "============================================================"

python3 campaigns/run_cloudlab_campaign.py \
    --algorithm lp \
    --phase chunk_probe \
    --campaign-root "${LP_CAMPAIGN_ROOT}" \
    --burst-partitions "${BURST_PARTITIONS}" \
    --spark-partitions "${SPARK_PARTITIONS}" \
    --spark-total-executors "${SPARK_EXECUTORS}" \
    --spark-config-memories "${SPARK_CONFIG_MEMORIES}" \
    --size-nodes "${SIZE_NODES}" \
    --config-runs "${CONFIG_RUNS}" \
    --size-runs "${SIZE_RUNS}" \
    "${OW_FLAGS[@]}" \
    2>&1 | tee "${LP_CAMPAIGN_ROOT}/logs/chunk_probe_rerun.log"

echo ""
echo "LP chunk probe complete."
echo ""

# ---------------------------------------------------------------------------
# Phase 2: BFS — full campaign
# ---------------------------------------------------------------------------
echo "============================================================"
echo "Phase 2: BFS — full campaign"
echo "============================================================"

python3 campaigns/run_cloudlab_campaign.py \
    --algorithm bfs \
    --phase full \
    --burst-partitions "${BURST_PARTITIONS}" \
    --spark-partitions "${SPARK_PARTITIONS}" \
    --spark-total-executors "${SPARK_EXECUTORS}" \
    --spark-config-memories "${SPARK_CONFIG_MEMORIES}" \
    --size-nodes "${SIZE_NODES}" \
    --config-runs "${CONFIG_RUNS}" \
    --size-runs "${SIZE_RUNS}" \
    "${OW_FLAGS[@]}" \
    2>&1 | tee "experiment_data/cloudlab_campaigns/bfs_campaign.log"

echo ""
echo "BFS campaign complete."
echo ""

# ---------------------------------------------------------------------------
# Phase 3: SSSP — full campaign
# ---------------------------------------------------------------------------
echo "============================================================"
echo "Phase 3: SSSP — full campaign"
echo "============================================================"

python3 campaigns/run_cloudlab_campaign.py \
    --algorithm sssp \
    --phase full \
    --burst-partitions "${BURST_PARTITIONS}" \
    --spark-partitions "${SPARK_PARTITIONS}" \
    --spark-total-executors "${SPARK_EXECUTORS}" \
    --spark-config-memories "${SPARK_CONFIG_MEMORIES}" \
    --size-nodes "${SIZE_NODES}" \
    --config-runs "${CONFIG_RUNS}" \
    --size-runs "${SIZE_RUNS}" \
    "${OW_FLAGS[@]}" \
    2>&1 | tee "experiment_data/cloudlab_campaigns/sssp_campaign.log"

echo ""
echo "SSSP campaign complete."
echo ""

echo "============================================================"
echo "ALL CAMPAIGNS COMPLETE"
echo "============================================================"
echo ""
echo "Campaign data:"
echo "  LP:   ${LP_CAMPAIGN_ROOT}"
echo "  BFS:  experiment_data/cloudlab_campaigns/campaign-bfs-*"
echo "  SSSP: experiment_data/cloudlab_campaigns/campaign-sssp-*"
