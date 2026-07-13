#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <input-path-in-container> <output-path-in-container> <num-nodes> <max-passes> <min-gain> <partitions>" >&2
  exit 1
fi

INPUT_PATH="$1"
OUTPUT_PATH="$2"
NUM_NODES="$3"
MAX_PASSES="$4"
MIN_GAIN="$5"
PARTITIONS="$6"
SCRIPT_PATH="/opt/tfm-spark/scripts/louvain_graphx_shell.scala"

if [ -d "${OUTPUT_PATH}" ]; then
  docker exec spark-master rm -rf "${OUTPUT_PATH}"
fi

docker exec \
  -e SPARK_TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}" \
  -e SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}" \
  -e SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}" \
  -e SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-12g}" \
  -e SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-4}" \
  -e SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}" \
  -e SPARK_PERSIST_OUTPUT="${SPARK_PERSIST_OUTPUT:-true}" \
  spark-master \
  /opt/spark/bin/spark-shell \
  --master spark://spark-master:7077 \
  --conf spark.executor.instances="${SPARK_TOTAL_EXECUTOR_CORES:-4}" \
  --conf spark.executor.cores="${SPARK_EXECUTOR_CORES:-1}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-4g}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-12g}" \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM:-4}" \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-4}" \
  --driver-java-options "-Dtfm.input=${INPUT_PATH} -Dtfm.output=${OUTPUT_PATH} -Dtfm.num_nodes=${NUM_NODES} -Dtfm.max_passes=${MAX_PASSES} -Dtfm.min_gain=${MIN_GAIN} -Dtfm.partitions=${PARTITIONS} -Dtfm.persist=${SPARK_PERSIST_OUTPUT:-true}" \
  -i "${SCRIPT_PATH}"
