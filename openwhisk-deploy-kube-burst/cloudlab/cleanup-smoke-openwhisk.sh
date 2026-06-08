#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="${OW_NAMESPACE:-openwhisk}"
RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"
PID_FILE="${OW_PORT_FORWARD_PID_FILE:-/tmp/${RELEASE_NAME}-port-forward.pid}"

if [[ -f "${PID_FILE}" ]]; then
  if kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
    kill "$(cat "${PID_FILE}")"
  fi
  rm -f "${PID_FILE}"
fi

helm uninstall "${RELEASE_NAME}" --namespace "${NAMESPACE}" >/dev/null 2>&1 || true
kubectl delete namespace "${NAMESPACE}" --ignore-not-found=true

