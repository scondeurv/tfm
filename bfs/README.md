# BFS

Burst vs Spark for Breadth-First Search. Used by the multi-algorithm CloudLab campaign.

## Required artifacts (campaign)

| File | Purpose |
|------|---------|
| `bfs.py`, `bfs_utils.py` | BFS source (algorithm + payload helpers used by tests) |
| `bfs.zip` | Burst action zip (built by `compile_bfs_cluster.sh`) |
| `benchmark_bfs.py` | Entry point invoked by `campaigns/run_cloudlab_campaign.py` |
| `setup_large_bfs_data.py` | Synthetic BFS graph generator |
| `run_cloudlab_smoke_bfs.sh` | Burst smoke test on CloudLab |
| `run_cloudlab_smoke_bfs_spark.sh` | Spark smoke test on CloudLab |
| `compile_bfs_cluster.sh` | Rebuild `bfs.zip` from `ow-bfs/` |
| `ow_client/` | OpenWhisk client used by `benchmark_bfs.py` |

## Running the campaign

```
campaigns/run_cloudlab_campaign.py --algorithm bfs --phase full
```

Replica (size sweep only, reuses winners):

```
campaigns/launch_replicas.sh replica4
```

## Manual data generation

```
python3 setup_large_bfs_data.py --nodes 1000000 --partitions 4 --no-s3 --density 10
```

S3 partitioning + upload is handled by the campaign runner. Shared data utilities live in `data_utils/`.
