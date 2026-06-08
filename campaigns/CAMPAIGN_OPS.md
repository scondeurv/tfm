# Campaign operations playbook

End-to-end runbook for the unified CloudLab campaign launcher
(`launch_campaign_v3.sh` → `run_cloudlab_campaign.py`). Covers idempotency,
resource profiling, timeouts, and verification. See
[REPRODUCTION.md](REPRODUCTION.md) for cluster pre-requisites and override env
vars.

## TL;DR

```bash
# Build all COST backends on compute6/7 first (one-time per release).
ssh sconde@cloudfunctions.urv.cat \
    "cd src && bash labelpropagation/compile_lp_cost_backends.sh && \
     bash bfs/compile_bfs_cost_backends.sh && \
     bash sssp/compile_sssp_cost_backends.sh && \
     bash pagerank/compile_pagerank_cost_backends.sh"

# Launch full unified campaign (lp, bfs, sssp, pagerank × 5 backends).
# Defaults match campaign-unified-20260524T064518Z; override via env vars.
bash campaigns/launch_campaign_v3.sh
```

## Phases

| Phase | Backends | Output |
|---|---|---|
| `preflight` | burst (smoke) | `preflight/preflight.json` |
| `characterization` | burst (SLA per partition) | `characterization/probes_p{p}.json` |
| `config_sweep` | burst (granularity + memory) | `config_sweep/best_config_p{p}.json` |
| `chunk_probe` | burst (S3 chunk size) | `chunk_probe/chunk_probe.json` |
| `size_sweep` | burst + spark | `size_sweep/burst_runs_p{p}.json`, `spark_runs.json` |
| `cost_sweep` | standalone + rayon + mpi | `cost_sweep/runs_{backend}.json` + `summary.json` |
| `report` | none (post-hoc) | `report/{algo}/*` + `report/{summary,README}.md` |

The `--phase full` default runs all of the above end-to-end. Each phase is
**independently invocable** with `--phase <name>` to re-run a single stage
(useful for re-rendering reports after editing `report_generators.py`, or
re-running cost_sweep without paying the burst cold-start cost).

## Idempotency

The orchestrator caches per-cell raw outputs and skips repeated work:

| Cache | Location | Trigger |
|---|---|---|
| Synthetic graph files | `<campaign_root>/datasets/large_<algo>_<n>.txt` | Regenerated only if absent or zero-sized |
| S3 partitioned graphs | `s3://<bucket>/cloudlab/campaigns/<root>/burst/<algo>/` | Skipped if `remote_lp_partitions_available()` returns True |
| Burst raw runs | `raw_runs/burst/burst_<algo>_n<n>_p<p>_g<g>_m<m>_ck<chunk>_run<i>.json` | Cached record reused; `compute_only_ms_proxy` backfilled if missing |
| Spark raw runs | `raw_runs/spark/spark_<algo>_n<n>_e<e>_m<m>_run<i>.json` | Same |
| Cost raw runs | `raw_runs/cost-{standalone,rayon,mpi}/cost_<algo>_<backend>_n<n>_<variant>_run<i>.json` | Skipped on second run; safe to interrupt + resume |

To force a re-run of a single cell, delete its raw_runs JSON before the next
invocation. To force a fresh campaign, point `--campaign-root` to a new path.

## Resource profiling

The orchestrator captures `kubectl top node` + `kubectl top pod` snapshots
before and after every burst/cost cell, written to
`resource_snapshots/<phase>/<snap_label>_{pre,post}.json`. These let you:

1. Confirm Burst pods stayed under their memory/CPU cap (`compute_only_ms_proxy`
   helps disambiguate "slow worker" vs "stuck on OW queue").
2. Validate that standalone/Rayon cells used the right thread count (`top -H -p
   <pid>` style data on compute6).
3. Detect noisy-neighbour interference between phases (e.g. a Spark run
   leaving residual JVM heap that throttles a subsequent Burst pod).

## Per-backend timeouts

Default `--cost-cell-timeout-sec 1800` (30 min). Per-backend overrides:

```bash
--cost-timeout-standalone-sec 5400   # n=10M LP standalone can take 1.5–2 min × safety
--cost-timeout-rayon-sec 1200        # 32-thread saturates compute6 quickly
--cost-timeout-mpi-sec 2400          # cross-host 1 GbE latency dominates at p=32
```

Default policy (auto): standalone cells with `n ≥ 5,000,000` get 3× the base
timeout (~90 min) since the serial baseline pays the full cost.

## Binary validation (pre-flight gate)

Pass `--validate-binaries` to make the orchestrator probe each Rust binary via
SSH **before** the first cell executes. Catches the common error of forgetting
to re-run the compile scripts after a code change. Example failure:

```
[cost_sweep] required Rust binaries not found on CloudLab host.
Re-run the compile scripts on compute6/7 first. Missing:
  mpi: /home/users/sconde/src/pagerank/pagerank-mpi/target/release/pagerank-mpi
```

## Real-world dataset support

See [REAL_DATASETS.md](REAL_DATASETS.md) for SNAP dataset workflow and
`--external-graph-tsv` / `--external-graph-num-nodes` semantics.

## Resilience (run-it-in-one-shot)

The orchestrator now prevents the failure modes that used to need bespoke
`resume_*.sh` scripts:

- **Preflight health gate** (`preflight_gate.py`): before any work, and once per
  algo invocation, it checks controller/invoker/couchdb/kafka/nginx readiness,
  the couchdb init marker, leftover guest pods, and foreign-tenant node load —
  then **auto-remediates** (reaps zombie pods, re-runs `init-couchdb` + bounces
  the control plane if the marker 404 is present, waits for Ready). Aborts with a
  diagnostic only if the cluster is saturated by a foreign tenant or unrecoverable.
  Flags: `--skip-preflight`, `--preflight-detect-only`, `--preflight-ready-timeout-sec`.
- **Per-cell resource fit-gate** (`burst_memory_mb` + `burst_cell_fit` in
  `cloudlab_common.py`): memory is scaled by graph size n (no more flat m=4096
  OOM at 10M) and every cell is checked against a single node's budget BEFORE
  launch. Infeasible cells are recorded `status=blocked` and skipped, never OOM
  mid-run.
- **`--dry-run`**: prints the full planned matrix with each cell's resource
  request + fit result and exits without touching the cluster. Run this first.
- **Orphan cleanup**: SIGINT/SIGTERM and normal exit tear down the kubectl
  port-forward, Spark apps/JVMs, MPI `orted`/`mpirun` on every host, and guest
  pods (`cleanup_all`). Ctrl-C no longer leaves the cluster dirty for the next run.
- **Cache integrity**: corrupt/truncated cached JSON (from an interrupted write)
  now reads as a cache miss and regenerates, for burst, **spark, and cost**.
- **Resume**: `--resume` (or just re-run the same command) skips cached PASSED
  cells and re-runs failed/blocked ones. The end-of-run `[health]` scan lists
  every non-passed cell and prints the exact resume command.

## Failure recovery

| Symptom | Action |
|---|---|
| Any cell failed/blocked | See the `[health]` scan at the end of the run; re-run with `--resume` (cached passes skip) |
| Controller/invoker wedged (couchdb 404) | Auto-remediated by the preflight gate; if it still aborts, the couchdb base DBs may be corrupt (manual re-init) |
| Burst run "transport error" | Retried at the HTTP layer (connect retries) + once at cell level; if still fails, `--resume` |
| MPI timeout on cross-host | Verify `--mpi-btl-if-include 192.168.5.0/24` matches the cluster's private subnet |
| Standalone OOM at n=10M | Burst memory is now n-scaled + fit-gated; for cost backends check `resource_snapshots/cost_sweep/*_pre.json` vs `compute6` 64 GB |
| Cluster left dirty after Ctrl-C | `cleanup_all` runs on signal; to clean manually: `CLEAN_BURST_PURGE_ACTIVATIONS=1 ./clean_burst_cluster.sh` |
| Report PNGs missing | Run `python3 campaigns/run_cloudlab_campaign.py --algorithm lp --campaign-root <root> --phase report` |
| `_consolidated.json` stale | Delete it; orchestrator no longer writes one — per-campaign metadata lives in `<root>/metadata.json` |

## Verification checklist

After a full campaign:

```bash
# 1. Raw runs populated for every backend.
find experiment_data/cloudlab_campaigns/campaign-unified-*/raw_runs -name "*.json" | wc -l

# 2. Report rendered for every algorithm.
find experiment_data/cloudlab_campaigns/campaign-unified-*/report -name "cost_table.md" | wc -l

# 3. No null compute_only_ms in burst records (proxy should have filled).
jq '.result.burst.compute_only_ms' \
    experiment_data/cloudlab_campaigns/campaign-unified-*/raw_runs/burst/*.json \
    | grep -c null   # expected: 0

# 4. All cross-backend tests still green.
.venv-tfm/bin/python -m unittest discover -s tests
```
