#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CHART_DIR="${ROOT_DIR}/helm/openwhisk"
VALUES_FILE="${SCRIPT_DIR}/owdev-cloudlab-values.yaml"

NAMESPACE="${OW_NAMESPACE:-openwhisk}"
RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"
RABBITMQ_URI="${OW_RABBITMQ_URI:-amqp://admin:admin@rabbitmq.burst-communication.svc.cluster.local:5672}"
REDIS_URI="redis://${RELEASE_NAME}-redis.${NAMESPACE}.svc.cluster.local:6379"
API_HOST_NAME="${OW_API_HOST_NAME:-localhost}"
API_HOST_PORT="${OW_API_HOST_PORT:-31001}"
INVOKER_REPLICAS="${OW_INVOKER_REPLICAS:-2}"
HELM_TIMEOUT="${OW_HELM_TIMEOUT:-15m}"

for cmd in kubectl helm; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Missing required command: ${cmd}" >&2
    exit 1
  fi
done

core_label="$(kubectl get node compute5 -o jsonpath='{.metadata.labels.openwhisk-role}' 2>/dev/null || true)"
invoker6_label="$(kubectl get node compute6 -o jsonpath='{.metadata.labels.openwhisk-role}' 2>/dev/null || true)"
invoker7_label="$(kubectl get node compute7 -o jsonpath='{.metadata.labels.openwhisk-role}' 2>/dev/null || true)"

if [[ "${core_label}" != "core" ]]; then
  echo "compute5 does not have openwhisk-role=core" >&2
  exit 1
fi
if [[ "${invoker6_label}" != "invoker" || "${invoker7_label}" != "invoker" ]]; then
  echo "compute6/compute7 do not have openwhisk-role=invoker" >&2
  exit 1
fi

kubectl -n burst-communication get svc rabbitmq >/dev/null

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --wait \
  --timeout "${HELM_TIMEOUT}" \
  -f "${VALUES_FILE}" \
  --set-string "whisk.middleware.rabbitmq=${RABBITMQ_URI}" \
  --set-string "whisk.middleware.redisList=${REDIS_URI}" \
  --set-string "whisk.middleware.redisStream=${REDIS_URI}" \
  --set-string "whisk.ingress.apiHostName=${API_HOST_NAME}" \
  --set "whisk.ingress.apiHostPort=${API_HOST_PORT}" \
  --set "invoker.containerFactory.kubernetes.replicaCount=${INVOKER_REPLICAS}" \
  --set "benchmark.workerPolicy.workers=${INVOKER_REPLICAS}"

kubectl -n "${NAMESPACE}" rollout status deployment/"${RELEASE_NAME}"-controller --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status deployment/"${RELEASE_NAME}"-nginx --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status deployment/"${RELEASE_NAME}"-couchdb --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status deployment/"${RELEASE_NAME}"-redis --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status deployment/"${RELEASE_NAME}"-apigateway --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status statefulset/"${RELEASE_NAME}"-kafka --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status statefulset/"${RELEASE_NAME}"-zookeeper --timeout="${HELM_TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status statefulset/"${RELEASE_NAME}"-invoker --timeout="${HELM_TIMEOUT}"

kubectl get pods -n "${NAMESPACE}" -o wide
