# V-cost + NUMA Extension Scripts (2026-06-15)

Scripts that produced the V-cost + NUMA campaigns extending the TFM
director-feedback analysis to three SNAP graphs and PageRank causal profiling.

## Runners (executed on compute6)

- `extra_datasets_runner_v2.py` — V-cost + NUMA sweep on com-orkut, web-Google,
  roadNet-CA. v2 fixes vs v1: `n = max_id+1` autodetect (SNAP is 1-indexed with
  gaps), source = highest-out-degree vertex (vertex 0 may have zero out-edges),
  MPI single-host only (cross-host 1 GbE saturates on big graphs), 900 s per-cell
  timeout, JSONL incremental write per cell (no loss on crash), stderr logged on
  failure. Produces `data/extra_datasets/results_<ts>/{dataset}/{vcost,numa}.jsonl`.
- `pr_numa_profile.py` — PR NUMA causal profile on soc-LiveJournal1: 5 reps
  per config (naive-64, local-32, interleave-64, mpi-per-socket) with
  `/sys/devices/system/node/node*/numastat` deltas captured per run. Produces
  `data/pr_numa_profile.json`.

## Analysis (executed on laptop)

- `summarize_extra.py` — print V-cost + NUMA + COST tables per dataset.
- `plot_extra.py` — 6 figures: V-cost scaling per algo (3 panels per dataset),
  grouped-bar NUMA per dataset, and cross-dataset COST summary.
- `plot_pr_numa_profile.py` — 2-panel figure (exec time + numastat alloc) for
  the PR per-socket causal study.

## Provenance

These scripts are the salvaged-v2 implementation after the v1 run on 2026-06-14
silently produced zero usable results because of two preparation bugs in v1
(n declared as count instead of max-id+1, source 0 unreachable in SNAP graphs).
The v2 runner ran 468 / 468 cells in 1 h 47 m on 2026-06-15 with no failures.
