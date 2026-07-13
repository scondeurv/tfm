#!/usr/bin/env bash
# V-cost campaign: vertical (rayon 1..128 single-node) vs horizontal (MPI cross-host)
# on real graph soc-LiveJournal1. Backends standalone/rayon/mpi only (no Burst/Spark; OW down).
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f ./.env ]]; then set -a; . ./.env; set +a; fi

DATASETS_DIR="${DATASETS_DIR:-${HOME}/datasets}"
ROOT="experiment_data/cloudlab_campaigns/vcost-$(date -u +%Y%m%dT%H%M%SZ)"
N=4847571
PLAIN="${DATASETS_DIR}/soc-LiveJournal1.tsv"
WEIGHTED="${DATASETS_DIR}/soc-LiveJournal1-weighted.tsv"

COMMON=(
  --phase cost
  --backends standalone,rayon,mpi
  --external-graph-num-nodes "$N"
  --cost-sweep-nodes "$N"
  --rayon-threads 1,2,4,8,16,32,64,128
  --mpi-ranks 2,4,8,16,32,64
  --mpi-hosts compute6:32,compute7:32
  --mpi-map-by node
  --mpi-btl-if-include 192.168.5.0/24
  --mpi-prefix "${CLOUDLAB_MPI_PREFIX:-/home/users/sconde/opt/openmpi-4.1.5}"
  --cost-runs 3 --max-iter 20
  --cloudlab-host "${CLOUDLAB_HOST:-compute6}"
  --cloudlab-ssh-key "${CLOUDLAB_SSH_KEY:-${HOME}/.ssh/id_pc1}"
  --cloudlab-ssh-config "${CLOUDLAB_SSH_CONFIG:-${HOME}/.ssh/config}"
  --cloudlab-src-root "${CLOUDLAB_SRC_ROOT:-/home/users/sconde/src}"
  --campaign-root "$ROOT"
  --skip-preflight
)

echo "=== V-cost campaign root: $ROOT ==="
for algo in lp bfs pagerank sssp; do
  graph="$PLAIN"; [ "$algo" = sssp ] && graph="$WEIGHTED"
  echo "######## $algo ($graph) ########"
  python3 -u campaigns/run_cloudlab_campaign.py --algorithm "$algo" \
    --external-graph-tsv "$graph" "${COMMON[@]}" 2>&1
done
echo "=== V-cost ALL DONE: $ROOT ==="
echo "$ROOT" > /tmp/vcost_root.txt
