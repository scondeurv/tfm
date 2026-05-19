# Label Propagation

Burst vs Spark for Label Propagation. Used by the multi-algorithm CloudLab campaign.

## Required artifacts (campaign)

| File | Purpose |
|------|---------|
| `labelpropagation.py`, `labelpropagation_utils.py` | LP source (algorithm + payload helpers used by tests) |
| `labelpropagation.zip` | Burst action zip (built by `compile_lp_cluster.sh`) |
| `benchmark_lp.py` | Entry point invoked by `campaigns/run_cloudlab_campaign.py` |
| `runtime_metrics.py` | Phase breakdown / logical traffic estimator (imported by `benchmark_lp.py`) |
| `setup_large_lp_data.py` | Synthetic LP graph generator |
| `run_cloudlab_smoke_lp.sh` | Burst smoke test on CloudLab |
| `run_cloudlab_smoke_lp_spark.sh` | Spark smoke test on CloudLab |
| `compile_lp_cluster.sh` | Rebuild `labelpropagation.zip` from `ow-lp/` |
| `benchmark_runtime_probe.py`, `runtime_probe.zip`, `compile_runtime_probe_cluster.sh` | Runtime characterization probe |
| `spark_baseline/` | Spark LP job (Scala) |
| `ow-lp/`, `ow-runtime-probe/` | Burst Rust workers |
| `ow_client/` | OpenWhisk client used by `benchmark_lp.py` |

## Running the campaign

LP is run via the multi-algorithm campaign runner:

```
campaigns/run_cloudlab_campaign.py --algorithm lp --phase full
```

Replica (size sweep only, reuses winners):

```
campaigns/launch_replicas.sh replica4
```

## Manual data generation

```
python3 setup_large_lp_data.py --nodes 1000000 --partitions 4 --no-s3 --density 10
```

Datasets land in the campaign directory under `<campaign-root>/datasets/`. S3 partitioning + upload is handled by the campaign runner (no per-algorithm `upload_to_minio.py` needed; shared utilities live in `data_utils/`).

## Notes

- The packaged CloudLab campaign does not run output validation during the measurement sweep.
- Reports + figures are generated from `<campaign-root>/size_sweep/` JSONs by `doc-tfm/reportes/generar_graficas_multi.py` and `analisis_robustez.py`.
