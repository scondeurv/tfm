#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="${OW_NAMESPACE:-openwhisk}"
RELEASE_NAME="${OW_RELEASE_NAME:-owdev}"
LOCAL_PORT="${OW_LOCAL_PORT:-31001}"
PID_FILE="${OW_PORT_FORWARD_PID_FILE:-/tmp/${RELEASE_NAME}-port-forward.pid}"
LOG_FILE="${OW_PORT_FORWARD_LOG_FILE:-/tmp/${RELEASE_NAME}-port-forward.log}"

if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
  echo "Port-forward already running with pid $(cat "${PID_FILE}")"
  exit 0
fi

kubectl -n "${NAMESPACE}" port-forward "svc/${RELEASE_NAME}-nginx" "${LOCAL_PORT}:80" --address 127.0.0.1 >"${LOG_FILE}" 2>&1 &
echo $! >"${PID_FILE}"

for _ in $(seq 1 30); do
  if curl -s "http://127.0.0.1:${LOCAL_PORT}/" >/dev/null 2>&1; then
    echo "OpenWhisk API forwarded on http://127.0.0.1:${LOCAL_PORT}"
    exit 0
  fi
  sleep 1
done

if kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
  kill "$(cat "${PID_FILE}")"
fi
rm -f "${PID_FILE}"
echo "Timed out waiting for port-forward to become ready" >&2
exit 1
