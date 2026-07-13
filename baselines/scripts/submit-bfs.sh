#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 ]]; then
  echo "Usage: $0 <input-path-in-container> <output-path-in-container> <source-node> <max-levels> <partitions>" >&2
  exit 1
fi

INPUT_PATH="$1"
OUTPUT_PATH="$2"
SOURCE_NODE="$3"
MAX_LEVELS="$4"
PARTITIONS="$5"

SCRIPT_PATH="/opt/tfm-spark/scripts/bfs_graphx_shell.scala"
TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}"
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
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
  --driver-java-options "-Dtfm.input=$INPUT_PATH -Dtfm.output=$OUTPUT_PATH -Dtfm.source=$SOURCE_NODE -Dtfm.max_levels=$MAX_LEVELS -Dtfm.partitions=$PARTITIONS -Dtfm.persist=$PERSIST_OUTPUT" \
  -i "$SCRIPT_PATH"
