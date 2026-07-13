#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
K8S_DIR="${SCRIPT_DIR}/k8s"
LP_SCALA="${ROOT_DIR}/scripts/labelpropagation_graphx_shell.scala"

CLOUDLAB_USER="${CLOUDLAB_USER:-sconde}"
CLOUDLAB_HOST="${CLOUDLAB_HOST:-cloudfunctions.urv.cat}"
CLOUDLAB_SSH_KEY="${CLOUDLAB_SSH_KEY:-/home/sergio/.ssh/id_pc1}"
NAMESPACE="${SPARK_SMOKE_NAMESPACE:-spark-sconde-smoke}"
SPARK_IMAGE="${SPARK_S3A_IMAGE:-spark:3.5.7-scala2.12-java17-ubuntu}"
REMOTE_BASE="${REMOTE_BASE:-/tmp/${NAMESPACE}}"
MASTER_NODE="${SPARK_MASTER_NODE:-compute7}"
MASTER_REQUEST_CPU="${SPARK_MASTER_REQUEST_CPU:-1}"
MASTER_LIMIT_CPU="${SPARK_MASTER_LIMIT_CPU:-2}"
MASTER_REQUEST_MEMORY="${SPARK_MASTER_REQUEST_MEMORY:-2Gi}"
MASTER_LIMIT_MEMORY="${SPARK_MASTER_LIMIT_MEMORY:-4Gi}"
WORKER_COMPUTE6_REPLICAS="${SPARK_WORKER_COMPUTE6_REPLICAS:-1}"
WORKER_COMPUTE7_REPLICAS="${SPARK_WORKER_COMPUTE7_REPLICAS:-1}"
WORKER_CORES="${SPARK_WORKER_CORES:-2}"
WORKER_MEMORY="${SPARK_WORKER_MEMORY:-4g}"
WORKER_REQUEST_CPU="${SPARK_WORKER_REQUEST_CPU:-2}"
WORKER_LIMIT_CPU="${SPARK_WORKER_LIMIT_CPU:-2}"
WORKER_REQUEST_MEMORY="${SPARK_WORKER_REQUEST_MEMORY:-4Gi}"
WORKER_LIMIT_MEMORY="${SPARK_WORKER_LIMIT_MEMORY:-4Gi}"
ACCESS_KEY_B64="$(printf '%s' "${AWS_ACCESS_KEY_ID}" | base64 -w0)"
SECRET_KEY_B64="$(printf '%s' "${AWS_SECRET_ACCESS_KEY}" | base64 -w0)"

if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY before deploying Spark" >&2
  exit 1
fi

SSH_TARGET="${CLOUDLAB_USER}@${CLOUDLAB_HOST}"

ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" "mkdir -p '${REMOTE_BASE}/k8s'"
scp -i "${CLOUDLAB_SSH_KEY}" "${LP_SCALA}" "${SSH_TARGET}:${REMOTE_BASE}/labelpropagation_graphx_shell.scala"
scp -i "${CLOUDLAB_SSH_KEY}" "${K8S_DIR}/"*.yaml "${SSH_TARGET}:${REMOTE_BASE}/k8s/"

ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${SSH_TARGET}" "
  set -euo pipefail
  ACCESS_KEY=\$(printf '%s' '${ACCESS_KEY_B64}' | base64 -d)
  SECRET_KEY=\$(printf '%s' '${SECRET_KEY_B64}' | base64 -d)
  kubectl get node compute6 >/dev/null
  kubectl get node compute7 >/dev/null
  sed -e 's#__NAMESPACE__#${NAMESPACE}#g' \
      '${REMOTE_BASE}/k8s/namespace.yaml' | kubectl apply -f -
  kubectl -n '${NAMESPACE}' create secret generic spark-minio-creds \
    --from-literal=AWS_ACCESS_KEY_ID=\"\${ACCESS_KEY}\" \
    --from-literal=AWS_SECRET_ACCESS_KEY=\"\${SECRET_KEY}\" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n '${NAMESPACE}' create configmap spark-lp-scripts \
    --from-file=labelpropagation_graphx_shell.scala='${REMOTE_BASE}/labelpropagation_graphx_shell.scala' \
    --dry-run=client -o yaml | kubectl apply -f -
  for manifest in master-service.yaml master-deployment.yaml worker-compute6-deployment.yaml worker-compute7-deployment.yaml; do
    sed -e 's#__NAMESPACE__#${NAMESPACE}#g' \
        -e 's#__SPARK_IMAGE__#${SPARK_IMAGE}#g' \
        -e 's#__MASTER_NODE__#${MASTER_NODE}#g' \
        -e 's#__MASTER_REQUEST_CPU__#${MASTER_REQUEST_CPU}#g' \
        -e 's#__MASTER_LIMIT_CPU__#${MASTER_LIMIT_CPU}#g' \
        -e 's#__MASTER_REQUEST_MEMORY__#${MASTER_REQUEST_MEMORY}#g' \
        -e 's#__MASTER_LIMIT_MEMORY__#${MASTER_LIMIT_MEMORY}#g' \
        -e 's#__WORKER_COMPUTE6_REPLICAS__#${WORKER_COMPUTE6_REPLICAS}#g' \
        -e 's#__WORKER_COMPUTE7_REPLICAS__#${WORKER_COMPUTE7_REPLICAS}#g' \
        -e 's#__WORKER_CORES__#${WORKER_CORES}#g' \
        -e 's#__WORKER_MEMORY__#${WORKER_MEMORY}#g' \
        -e 's#__WORKER_REQUEST_CPU__#${WORKER_REQUEST_CPU}#g' \
        -e 's#__WORKER_LIMIT_CPU__#${WORKER_LIMIT_CPU}#g' \
        -e 's#__WORKER_REQUEST_MEMORY__#${WORKER_REQUEST_MEMORY}#g' \
        -e 's#__WORKER_LIMIT_MEMORY__#${WORKER_LIMIT_MEMORY}#g' \
        '${REMOTE_BASE}/k8s/'\"\${manifest}\" | kubectl apply -f -
  done
  kubectl -n '${NAMESPACE}' rollout status deploy/spark-master --timeout=180s
  kubectl -n '${NAMESPACE}' rollout status deploy/spark-worker-compute6 --timeout=180s
  kubectl -n '${NAMESPACE}' rollout status deploy/spark-worker-compute7 --timeout=180s
"
