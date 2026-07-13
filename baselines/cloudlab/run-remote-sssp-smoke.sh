#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:?namespace required}"
BUCKET="${2:?bucket required}"
INPUT_KEY="${3:?input key required}"
OUTPUT_PREFIX="${4:?output prefix required}"
S3_ENDPOINT="${5:?s3 endpoint required}"
SOURCE_NODE="${6:?source_node required}"
MAX_ITER="${7:?max_iter required}"
PARTITIONS="${8:?partitions required}"
TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}"
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-4}"
SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
APP_NAME="${SPARK_APP_NAME:-tfm-sssp-cell}"

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
    --driver-java-options '-Dtfm.input=s3a://${BUCKET}/${INPUT_KEY} -Dtfm.output=s3a://${BUCKET}/${OUTPUT_PREFIX} -Dtfm.source=${SOURCE_NODE} -Dtfm.max_iter=${MAX_ITER} -Dtfm.partitions=${PARTITIONS} -Dtfm.persist=true' \
    -i /opt/tfm-spark/scripts/sssp_graphx_shell.scala
"
