#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTHON_BIN="${LP_PYTHON_BIN:-${SCRIPT_DIR}/.venv/bin/python}"
NODES="${LP_SMOKE_NODES:-100000}"
PARTITIONS="${LP_SMOKE_PARTITIONS:-4}"
GRANULARITY="${LP_SMOKE_GRANULARITY:-2}"
ITERATIONS="${LP_SMOKE_ITERATIONS:-20}"
MEMORY_MB="${LP_SMOKE_MEMORY_MB:-4096}"
OW_HOST="${OW_HOST:-127.0.0.1}"
OW_PORT="${OW_PORT:-31001}"
OW_PROTOCOL="${OW_PROTOCOL:-http}"
OW_NAMESPACE="${OW_NAMESPACE:-openwhisk}"
OW_RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"
BUCKET="${LP_SMOKE_BUCKET:-tfm-smoke}"
KEY_PREFIX="${LP_SMOKE_KEY_PREFIX:-cloudlab/lp}"
WORKER_S3_ENDPOINT="${S3_WORKER_ENDPOINT:-http://192.168.5.24:9000}"
HOST_S3_ENDPOINT="${S3_HOST_ENDPOINT:-http://192.168.5.24:9000}"

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

if [[ ! -f "${SCRIPT_DIR}/labelpropagation.zip" ]]; then
  echo "Missing ${SCRIPT_DIR}/labelpropagation.zip; compile the Burst action first" >&2
  exit 1
fi

if [[ ! -x "${SCRIPT_DIR}/lpst/target/release/label-propagation" ]]; then
  echo "Missing standalone binary at ${SCRIPT_DIR}/lpst/target/release/label-propagation" >&2
  exit 1
fi

if ! curl -s "http://${OW_HOST}:${OW_PORT}/" >/dev/null 2>&1; then
  echo "OpenWhisk API is not reachable on http://${OW_HOST}:${OW_PORT}" >&2
  exit 1
fi

OPENWHISK_K8S_NAMESPACE="${OW_NAMESPACE}" \
OPENWHISK_RELEASE_NAME="${OW_RELEASE_NAME}" \
"${PYTHON_BIN}" "${SCRIPT_DIR}/benchmark_lp.py" \
  --nodes "${NODES}" \
  --partitions "${PARTITIONS}" \
  --granularity "${GRANULARITY}" \
  --iter "${ITERATIONS}" \
  --memory "${MEMORY_MB}" \
  --ow-host "${OW_HOST}" \
  --ow-port "${OW_PORT}" \
  --ow-protocol "${OW_PROTOCOL}" \
  --ow-k8s-namespace "${OW_NAMESPACE}" \
  --ow-release-name "${OW_RELEASE_NAME}" \
  --backend redis-list \
  --chunk-size 1024 \
  --s3-endpoint "${WORKER_S3_ENDPOINT}" \
  --bucket "${BUCKET}" \
  --key-prefix "${KEY_PREFIX}"
