#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="${SPARK_S3A_IMAGE:-ghcr.io/sconde/tfm-spark-s3a:3.5.7-hadoop3.3.4}"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile.spark-s3a"

docker build -t "${IMAGE_NAME}" -f "${DOCKERFILE}" "${SCRIPT_DIR}"

if [[ "${PUSH_IMAGE:-0}" == "1" ]]; then
  docker push "${IMAGE_NAME}"
fi
