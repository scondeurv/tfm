#!/usr/bin/env bash
# Replica launcher: re-runs size-sweep phase only for LP/BFS/SSSP/WCC.
# Reuses winners from original campaigns (config_sweep/best_config_p*.json)
# and writes new size_sweep/raw_runs into a fresh campaign root per replica.
#
# Usage:
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
#   campaigns/launch_replicas.sh <replica-tag>
#
# Example:
#   campaigns/launch_replicas.sh replica2
#
# Prerequisites: same as launch_full_campaign.sh (SSH to CloudLab,
# AWS_* env vars for MinIO).
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <replica-tag>" >&2
    echo "  e.g. $0 replica2" >&2
    exit 1
fi
REPLICA_TAG="$1"
TS="$(date -u +%Y%m%dT%H%M%SZ)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    echo "ERROR: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for CloudLab MinIO" >&2
    exit 1
fi

# Original campaigns (winners source)
declare -A ORIGIN=(
    [lp]="experiment_data/cloudlab_campaigns/campaign-lp-20260423T125204Z"
    [bfs]="experiment_data/cloudlab_campaigns/campaign-bfs-20260426T071507Z"
    [sssp]="experiment_data/cloudlab_campaigns/campaign-sssp-20260427T052407Z"
    [wcc]="experiment_data/cloudlab_campaigns/campaign-wcc-20260427T093005Z"
)

BURST_PARTITIONS="4,8,16"
SPARK_PARTITIONS="8"
SPARK_EXECUTORS="8"
SPARK_CONFIG_MEMORIES="4g,6g"
SIZE_NODES="100000,500000,1000000,2000000"
SIZE_RUNS=3

OW_NAMESPACE="${OW_NAMESPACE:-openwhisk}"
OW_RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"
OW_PORT="${OW_PORT:-31002}"

OW_FLAGS=(
    --ow-namespace "${OW_NAMESPACE}"
    --ow-release-name "${OW_RELEASE_NAME}"
    --ow-port "${OW_PORT}"
)

for ALGO in lp bfs sssp wcc; do
    SRC="${ORIGIN[$ALGO]}"
    # Reuse existing dir if present (resume), else create new
    EXISTING="$(ls -dt experiment_data/cloudlab_campaigns/${REPLICA_TAG}-${ALGO}-* 2>/dev/null | head -1 || true)"
    if [[ -n "${EXISTING}" ]]; then
        DEST="${EXISTING}"
        echo "Reusing existing dir for resume: ${DEST}"
    else
        DEST="experiment_data/cloudlab_campaigns/${REPLICA_TAG}-${ALGO}-${TS}"
    fi

    echo "============================================================"
    echo "Replica: ${ALGO}"
    echo "  origin:  ${SRC}"
    echo "  dest:    ${DEST}"
    echo "============================================================"

    mkdir -p "${DEST}/config_sweep" "${DEST}/logs" "${DEST}/size_sweep"

    # Seed winners from origin
    cp "${SRC}/config_sweep/"best_config_p*.json "${DEST}/config_sweep/"
    cp "${SRC}/config_sweep/"best_config.json "${DEST}/config_sweep/" 2>/dev/null || true

    # Note replica origin
    cat > "${DEST}/replica_metadata.json" <<EOF
{
  "replica_tag": "${REPLICA_TAG}",
  "origin_campaign": "${SRC}",
  "algorithm": "${ALGO}",
  "created_at_utc": "${TS}"
}
EOF

    python3 -u campaigns/run_cloudlab_campaign.py \
        --algorithm "${ALGO}" \
        --phase size \
        --campaign-root "${DEST}" \
        --burst-partitions "${BURST_PARTITIONS}" \
        --spark-partitions "${SPARK_PARTITIONS}" \
        --spark-total-executors "${SPARK_EXECUTORS}" \
        --spark-config-memories "${SPARK_CONFIG_MEMORIES}" \
        --size-nodes "${SIZE_NODES}" \
        --size-runs "${SIZE_RUNS}" \
        "${OW_FLAGS[@]}" \
        2>&1 | tee "${DEST}/logs/size_sweep.log"

    echo "Replica ${ALGO} complete: ${DEST}"
    echo ""
done

echo "============================================================"
echo "All replicas (${REPLICA_TAG}) complete."
echo "============================================================"
