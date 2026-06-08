# Running campaigns with real-world datasets

The unified orchestrator supports drop-in replacement of the synthetic dataset
generator with any TSV edge list via `--external-graph-tsv` +
`--external-graph-num-nodes`.

## Recommended datasets (SNAP, Stanford)

The COST paper (McSherry et al., HotOS '15) measures PageRank on
`soc-LiveJournal1`. Replicating against the same graph removes the "synthetic-
only" critique from the TFM defence.

| Dataset | Nodes | Edges | Size (gz) | Type | URL |
|---|---:|---:|---:|---|---|
| **soc-LiveJournal1** | 4,847,571 | 68,993,773 | ~260 MB | directed social | https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz |
| com-Orkut | 3,072,441 | 117,185,083 | ~700 MB | undirected community | https://snap.stanford.edu/data/com-Orkut.html |
| web-Google | 875,713 | 5,105,039 | ~22 MB | directed web | https://snap.stanford.edu/data/web-Google.txt.gz |
| cit-Patents | 3,774,768 | 16,518,948 | ~70 MB | directed citation (DAG) | https://snap.stanford.edu/data/cit-Patents.txt.gz |
| roadNet-CA | 1,965,206 | 5,533,214 | ~50 MB | undirected road | https://snap.stanford.edu/data/roadNet-CA.html |

## Workflow

```bash
# 1. Download + decompress.
mkdir -p ~/datasets
curl -L https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz \
  -o ~/datasets/soc-LiveJournal1.txt.gz
gunzip ~/datasets/soc-LiveJournal1.txt.gz

# 2. Strip the comment lines (SNAP TSVs start with '# ...').
grep -v '^#' ~/datasets/soc-LiveJournal1.txt > ~/datasets/soc-LiveJournal1.tsv

# 3. Launch a single-point campaign at the dataset's native node count.
python3 campaigns/run_cloudlab_campaign.py \
    --algorithm pagerank \
    --backends standalone,rayon,burst \
    --external-graph-tsv ~/datasets/soc-LiveJournal1.tsv \
    --external-graph-num-nodes 4847571 \
    --cost-sweep-nodes 4847571 \
    --size-nodes 4847571 \
    --burst-partitions 4,8,16 \
    --cost-runs 3 \
    --size-runs 3
```

## Notes

- SNAP files use 0-indexed vertex IDs that may be non-contiguous. The Rust CSR
  builder tolerates stray IDs (drops edges with `src >= num_nodes`) but a
  conservative choice is to set `--external-graph-num-nodes` to one above the
  maximum vertex ID observed in the file (`awk '{if ($1>m) m=$1; if ($2>m) m=$2} END{print m+1}'`).
- The node count must be passed because the algorithm cores allocate `num_nodes`-sized
  rank / label / level vectors at start-up; mismatching values cause out-of-bounds
  drops (LP/BFS/SSSP/PageRank all share this convention).
- For undirected datasets (`com-Orkut`, `roadNet-CA`), the file lists each edge
  once. The Rust binaries treat the input as directed; if symmetric semantics
  are needed (WCC-style), duplicate each edge: `awk '{print $1"\t"$2; print $2"\t"$1}'`.
- `--external-graph-tsv` only intercepts cells whose `nodes` count matches
  `--external-graph-num-nodes`. Other cells in the sweep still use the synthetic
  generator. To pin the campaign to a single real-world point, set
  `--cost-sweep-nodes <N>` and `--size-nodes <N>` to the same value.
