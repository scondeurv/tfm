#!/usr/bin/env bash
# V-cost on the 3 SNAP graphs, SINGLE-HOST MPI (compute6 only), backends std/rayon/mpi.
# Mirrors run_vcost.sh (soc-LiveJournal1, cross-host) for the extra-cost SNAP table.
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f ./.env ]]; then set -a; . ./.env; set +a; fi

DATASETS_DIR="${DATASETS_DIR:-${HOME}/datasets}"
ROOT="experiment_data/cloudlab_campaigns/snapcost-$(date -u +%Y%m%dT%H%M%SZ)"
echo "$ROOT" > /tmp/snapcost_root.txt
echo "=== SNAP V-cost root: $ROOT ==="

# graph -> num_nodes
# num_nodes = max node id + 1 (graphs are non-contiguous; using the SNAP node
# COUNT drops out-of-range edges and panics lp-mpi). Verified by awk max over both cols.
declare -A NN=( [web-Google]=916428 [roadNet-CA]=1971281 [com-orkut]=3072627 )

COMMON=(
  --phase cost
  --backends standalone,rayon,mpi
  --rayon-threads 1,2,4,8,16,32,64,128
  --mpi-ranks 2,4,8,16,32
  --mpi-hosts compute6:64
  --mpi-map-by core
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

for graph in web-Google roadNet-CA com-orkut; do
  N=${NN[$graph]}
  for algo in lp bfs pagerank sssp; do
    g="${DATASETS_DIR}/${graph}.tsv"; [ "$algo" = sssp ] && g="${DATASETS_DIR}/${graph}-weighted.tsv"
    echo "######## SNAP $graph / $algo (N=$N) $(date -u +%H:%M:%S) ########"
    python3 -u campaigns/run_cloudlab_campaign.py --algorithm "$algo" \
      --external-graph-tsv "$g" --external-graph-num-nodes "$N" \
      --cost-sweep-nodes "$N" "${COMMON[@]}" 2>&1 \
      | grep -E "health|fail|panic|Error|RuntimeError|blocked" | tail -2
  done
done
echo "=== SNAP_VCOST_ALL_DONE $(date -u +%H:%M:%S) : $ROOT ==="
