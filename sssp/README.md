# SSSP

Burst vs Spark for Single-Source Shortest Paths. Used by the multi-algorithm CloudLab campaign.

## Required artifacts (campaign)

| File | Purpose |
|------|---------|
| `sssp.py`, `sssp_utils.py` | SSSP source (algorithm + payload helpers used by tests) |
| `sssp.zip` | Burst action zip (built by `compile_sssp_cluster.sh`) |
| `benchmark_sssp.py` | Entry point invoked by `campaigns/run_cloudlab_campaign.py` |
| `setup_large_sssp_data.py` | Synthetic weighted-graph generator |
| `run_cloudlab_smoke_sssp.sh` | Burst smoke test on CloudLab |
| `run_cloudlab_smoke_sssp_spark.sh` | Spark smoke test on CloudLab |
| `compile_sssp_cluster.sh` | Rebuild `sssp.zip` from `ow-sssp/` |
| `ow_client/` | OpenWhisk client used by `benchmark_sssp.py` |

## Running the campaign

```
campaigns/run_cloudlab_campaign.py --algorithm sssp --phase full
```

Replica (size sweep only, reuses winners):

```
campaigns/launch_replicas.sh replica4
```

## Manual data generation

```
python3 setup_large_sssp_data.py --nodes 1000000 --partitions 4 --no-s3 --density 10 --max-weight 100
```

S3 partitioning + upload is handled by the campaign runner. Shared data utilities live in `data_utils/`.
