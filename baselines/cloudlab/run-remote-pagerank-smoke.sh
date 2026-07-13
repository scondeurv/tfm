#!/usr/bin/env bash
# PageRank Spark/GraphX remote smoke runner.
# Mirrors run-remote-lp-smoke.sh but invokes pagerank_graphx_shell.scala
# with PR-specific tunables (damping, tolerance).
set -euo pipefail

NAMESPACE="${1:?namespace required}"
BUCKET="${2:?bucket required}"
INPUT_KEY="${3:?input key required}"
OUTPUT_PREFIX="${4:?output prefix required}"
S3_ENDPOINT="${5:?s3 endpoint required}"
MAX_ITER="${6:?max_iter required}"
PARTITIONS="${7:?partitions required}"
DAMPING="${PR_SPARK_SMOKE_DAMPING:-0.85}"
TOLERANCE="${PR_SPARK_SMOKE_TOLERANCE:-1e-6}"
TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}"
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-4}"
SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
APP_NAME="${SPARK_APP_NAME:-tfm-pagerank-cell}"

MASTER_POD="$(kubectl -n "${NAMESPACE}" get pods -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
if [[ -z "${MASTER_POD}" ]]; then
  echo "No running Spark master pod found in namespace ${NAMESPACE}" >&2
  exit 1
fi

kubectl -n "${NAMESPACE}" exec "${MASTER_POD}" -- bash -lc "
  set -euo pipefail
  : \"\${AWS_ACCESS_KEY_ID:?missing AWS_ACCESS_KEY_ID}\"
  : \"\${AWS_SECRET_ACCESS_KEY:?missing AWS_SECRET_ACCESS_KEY}\"
  : \"\${POD_IP:?missing POD_IP}\"
  /opt/spark/bin/spark-shell \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name '${APP_NAME}' \
    --conf spark.app.name='${APP_NAME}' \
    --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.driver.host=\${POD_IP} \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
    --conf spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS} \
    --conf spark.driver.extraClassPath=/opt/spark/extra-jars/* \
    --conf spark.executor.extraClassPath=/opt/spark/extra-jars/* \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
    --conf spark.hadoop.fs.s3a.access.key=\${AWS_ACCESS_KEY_ID} \
    --conf spark.hadoop.fs.s3a.secret.key=\${AWS_SECRET_ACCESS_KEY} \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --driver-java-options '-Dtfm.input=s3a://${BUCKET}/${INPUT_KEY} -Dtfm.output=s3a://${BUCKET}/${OUTPUT_PREFIX} -Dtfm.max_iter=${MAX_ITER} -Dtfm.partitions=${PARTITIONS} -Dtfm.damping=${DAMPING} -Dtfm.tolerance=${TOLERANCE} -Dtfm.persist=true' \
    -i /opt/tfm-spark/scripts/pagerank_graphx_shell.scala
"
