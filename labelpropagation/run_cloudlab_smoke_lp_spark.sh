#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${LP_PYTHON_BIN:-${SCRIPT_DIR}/.venv/bin/python}"

CLOUDLAB_USER="${CLOUDLAB_USER:-sconde}"
CLOUDLAB_HOST="${CLOUDLAB_HOST:-cloudfunctions.urv.cat}"
CLOUDLAB_SSH_KEY="${CLOUDLAB_SSH_KEY:-/home/sergio/.ssh/id_pc1}"
SPARK_NAMESPACE="${SPARK_SMOKE_NAMESPACE:-spark-sconde-smoke}"
S3_ENDPOINT="${S3_ENDPOINT:-http://192.168.5.24:9000}"
BUCKET="${SPARK_SMOKE_BUCKET:-tfm-smoke}"
INPUT_KEY="${SPARK_SMOKE_INPUT_KEY:-cloudlab/spark/labelpropagation/large_100000.txt}"
OUTPUT_PREFIX="${SPARK_SMOKE_OUTPUT_PREFIX:-cloudlab/spark/labelpropagation/large-100000/output}"
MAX_ITER="${LP_SPARK_SMOKE_ITERATIONS:-20}"
PARTITIONS="${LP_SPARK_SMOKE_PARTITIONS:-4}"
GRAPH_FILE="${LP_SPARK_SMOKE_GRAPH_FILE:-${SCRIPT_DIR}/large_100000.txt}"
NODES="${LP_SPARK_SMOKE_NODES:-100000}"
SPARK_TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-${SPARK_TOTAL_EXECUTOR_CORES}}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-${SPARK_TOTAL_EXECUTOR_CORES}}"
REMOTE_RUNNER="${REMOTE_RUNNER:-/tmp/${SPARK_NAMESPACE}/run-remote-lp-smoke.sh}"
REMOTE_BASE="${REMOTE_BASE:-/tmp/${SPARK_NAMESPACE}}"
REMOTE_GRAPH_FILE="${REMOTE_BASE}/$(basename "${GRAPH_FILE}")"
REMOTE_LOG="${REMOTE_BASE}/spark-lp-smoke.log"
REMOTE_OUTPUT_DIR="/tmp/${SPARK_NAMESPACE}-output"
REMOTE_OUTPUT_TAR="/tmp/${SPARK_NAMESPACE}-output.tgz"
LOCAL_OUTPUT_BASE="$(mktemp -d /tmp/${SPARK_NAMESPACE}-local-XXXXXX)"
LOCAL_OUTPUT_TAR="${LOCAL_OUTPUT_BASE}/output.tgz"
LOCAL_OUTPUT_DIR="${LOCAL_OUTPUT_BASE}/extracted"
LOCAL_RUN_LOG="${LOCAL_OUTPUT_BASE}/remote-run.log"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    echo "Missing Python interpreter at ${PYTHON_BIN} and python3 is not available" >&2
    exit 1
  fi
fi

if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for the CloudLab MinIO instance" >&2
  exit 1
fi

cleanup_local() {
  rm -rf "${LOCAL_OUTPUT_BASE}"
}
trap cleanup_local EXIT

if [[ ! -f "${GRAPH_FILE}" ]]; then
  echo "Missing graph file ${GRAPH_FILE}" >&2
  exit 1
fi

bash "${ROOT_DIR}/baselines/cloudlab/deploy-spark-lp-smoke.sh"

SSH_TARGET="${CLOUDLAB_USER}@${CLOUDLAB_HOST}"
ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" "mkdir -p '${REMOTE_BASE}'"
scp -i "${CLOUDLAB_SSH_KEY}" \
  "${ROOT_DIR}/baselines/cloudlab/run-remote-lp-smoke.sh" \
  "${SSH_TARGET}:${REMOTE_RUNNER}"
scp -i "${CLOUDLAB_SSH_KEY}" \
  "${GRAPH_FILE}" \
  "${SSH_TARGET}:${REMOTE_GRAPH_FILE}"
ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" "chmod +x '${REMOTE_RUNNER}'"

ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" "python3 - <<'PY'
import boto3
s3 = boto3.client(
    's3',
    endpoint_url='${S3_ENDPOINT}',
    aws_access_key_id='${AWS_ACCESS_KEY_ID}',
    aws_secret_access_key='${AWS_SECRET_ACCESS_KEY}',
)
s3.upload_file('${REMOTE_GRAPH_FILE}', '${BUCKET}', '${INPUT_KEY}')
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='${BUCKET}', Prefix='${OUTPUT_PREFIX}'):
    for item in page.get('Contents', []):
        s3.delete_object(Bucket='${BUCKET}', Key=item['Key'])
print('uploaded')
PY" >/dev/null

ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" \
  "SPARK_TOTAL_EXECUTOR_CORES='${SPARK_TOTAL_EXECUTOR_CORES}' SPARK_EXECUTOR_CORES='${SPARK_EXECUTOR_CORES}' SPARK_EXECUTOR_MEMORY='${SPARK_EXECUTOR_MEMORY}' SPARK_DEFAULT_PARALLELISM='${SPARK_DEFAULT_PARALLELISM}' SPARK_SHUFFLE_PARTITIONS='${SPARK_SHUFFLE_PARTITIONS}' '${REMOTE_RUNNER}' '${SPARK_NAMESPACE}' '${BUCKET}' '${INPUT_KEY}' '${OUTPUT_PREFIX}' '${S3_ENDPOINT}' '${MAX_ITER}' '${PARTITIONS}'" \
  | tee "${LOCAL_RUN_LOG}"

RESULT_JSON="$(grep 'SPARK_BENCHMARK_RESULT_JSON:' "${LOCAL_RUN_LOG}" | tail -n1 | sed 's/^.*SPARK_BENCHMARK_RESULT_JSON://')"
if [[ -z "${RESULT_JSON}" ]]; then
  echo "Spark smoke test did not emit a structured benchmark result" >&2
  exit 1
fi

if [[ "${SPARK_SKIP_VALIDATION:-false}" == "true" ]]; then
  printf '%s\n' "${RESULT_JSON}"
  exit 0
fi

ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" "rm -rf '${REMOTE_OUTPUT_DIR}' '${REMOTE_OUTPUT_TAR}' && mkdir -p '${REMOTE_OUTPUT_DIR}' && python3 - <<'PY'
import boto3
from pathlib import Path
bucket = '${BUCKET}'
prefix = '${OUTPUT_PREFIX}'
out = Path('${REMOTE_OUTPUT_DIR}')
s3 = boto3.client(
    's3',
    endpoint_url='${S3_ENDPOINT}',
    aws_access_key_id='${AWS_ACCESS_KEY_ID}',
    aws_secret_access_key='${AWS_SECRET_ACCESS_KEY}',
)
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
    for item in page.get('Contents', []):
        key = item['Key']
        rel = key[len(prefix):].lstrip('/')
        if not rel:
            continue
        target = out / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(target))
print('downloaded')
PY
cd /tmp && tar -czf '${REMOTE_OUTPUT_TAR}' '$(basename "${REMOTE_OUTPUT_DIR}")'"

scp -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}:${REMOTE_OUTPUT_TAR}" "${LOCAL_OUTPUT_TAR}"
mkdir -p "${LOCAL_OUTPUT_DIR}"
tar -xzf "${LOCAL_OUTPUT_TAR}" -C "${LOCAL_OUTPUT_DIR}"

VALIDATION_JSON="$("${PYTHON_BIN}" "${SCRIPT_DIR}/cloudlab_lp_spark_smoke.py" validate-local-output \
  --local-output-dir "${LOCAL_OUTPUT_DIR}/$(basename "${REMOTE_OUTPUT_DIR}")" \
  --graph-file "${GRAPH_FILE}" \
  --num-nodes "${NODES}" \
  --max-iter "${MAX_ITER}" || true)"

printf '%s\n' "${RESULT_JSON}"
printf '%s\n' "${VALIDATION_JSON}"
