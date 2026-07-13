#!/usr/bin/env bash
# NUMA placement study on all 4 real graphs (soc-LiveJournal1 + 3 SNAP).
# Runs numa_experiment.py per graph (env-parameterized). No OW. compute6 only.
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="experiment_data/numa/numa-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$ROOT"
echo "$ROOT" > /tmp/numa_root.txt
echo "=== NUMA root: $ROOT ==="
# Dataset dir ON the CloudLab host (numa_experiment.py runs remotely over SSH).
D="${CLOUDLAB_DATASETS_DIR:-/home/users/sconde/datasets}"
run() {  # name num_nodes
  local g="$1" n="$2"
  echo "######## NUMA $g (N=$n) $(date -u +%H:%M:%S) ########"
  GRAPH_PLAIN="$D/${g}.tsv" GRAPH_WEIGHTED="$D/${g}-weighted.tsv" NUM_NODES="$n" \
    python3 -u numa_experiment.py "$ROOT/numa_${g}.json" 2>&1 \
    | grep -E '=>|WROTE|ERR|failed' | tail -20
}
run soc-LiveJournal1 4847571
run web-Google       916428
run roadNet-CA       1971281
run com-orkut        3072627
echo "=== NUMA_ALL_DONE $(date -u +%H:%M:%S) : $ROOT ==="
