# CloudLab Spark Smoke Test

This folder contains the CloudLab-specific Spark deployment path used for a
temporary, namespaced LP smoke test.

Design constraints:

- no cluster-scoped resources
- no changes to shared Kubernetes infrastructure
- no PVC/PV, Ingress, NodePort or NetworkPolicy
- everything isolated to a dedicated namespace

Main entry points:

- `build-spark-s3a-image.sh`: build and optionally push the Spark image with
  `s3a` support
- `deploy-spark-lp-smoke.sh`: deploy a temporary Spark standalone cluster in a
  namespaced CloudLab namespace
- `cleanup-spark-lp-smoke.sh`: remove that namespace
- `run-remote-lp-smoke.sh`: run the LP Spark job from the CloudLab proxy after
  deployment

The public smoke-test wrapper lives in:

- `/home/sergio/src/labelpropagation/run_cloudlab_smoke_lp_spark.sh`

Important deployment knobs:

- `SPARK_MASTER_NODE`
- `SPARK_MASTER_REQUEST_CPU`, `SPARK_MASTER_LIMIT_CPU`
- `SPARK_MASTER_REQUEST_MEMORY`, `SPARK_MASTER_LIMIT_MEMORY`
- `SPARK_WORKER_COMPUTE6_REPLICAS`, `SPARK_WORKER_COMPUTE7_REPLICAS`
- `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`
- `SPARK_WORKER_REQUEST_CPU`, `SPARK_WORKER_LIMIT_CPU`
- `SPARK_WORKER_REQUEST_MEMORY`, `SPARK_WORKER_LIMIT_MEMORY`
- `SPARK_TOTAL_EXECUTOR_CORES`, `SPARK_EXECUTOR_CORES`, `SPARK_EXECUTOR_MEMORY`
- `SPARK_DEFAULT_PARALLELISM`, `SPARK_SHUFFLE_PARTITIONS`

This lets the same namespaced Spark deployment path serve both the exact smoke
test and a larger LP campaign without introducing new shared-cluster resources.
