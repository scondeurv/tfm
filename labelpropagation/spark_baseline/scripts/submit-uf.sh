#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <input-path-in-container> <output-path-in-container> <partitions>" >&2
  exit 1
fi

INPUT_PATH="$1"
OUTPUT_PATH="$2"
PARTITIONS="$3"

SCRIPT_PATH="/opt/tfm-spark/scripts/connected_components_shell.scala"
TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}"
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-4}"
SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
PERSIST_OUTPUT="${SPARK_PERSIST_OUTPUT:-false}"

docker exec spark-master /opt/spark/bin/spark-shell \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --total-executor-cores "$TOTAL_EXECUTOR_CORES" \
  --executor-cores "$EXECUTOR_CORES" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --conf spark.default.parallelism="$DEFAULT_PARALLELISM" \
  --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTITIONS" \
  --conf spark.task.cpus=1 \
  --driver-java-options "-Dtfm.input=$INPUT_PATH -Dtfm.output=$OUTPUT_PATH -Dtfm.partitions=$PARTITIONS -Dtfm.persist=$PERSIST_OUTPUT" \
  -i "$SCRIPT_PATH"
