#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="${1:-${CLEAN_BURST_NAMESPACE:-openwhisk}}"
RELEASE_NAME="${2:-${CLEAN_BURST_RELEASE_NAME:-owdev}}"
TIMEOUT_SECONDS="${CLEAN_BURST_TIMEOUT_SECONDS:-90}"
POD_PREFIX="pod/wsk${RELEASE_NAME}-invoker-"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found; cannot clean Burst cluster" >&2
  exit 1
fi

mapfile -t PODS < <(
  kubectl get pods -n "$NAMESPACE" -o name 2>/dev/null \
    | grep "^${POD_PREFIX}" \
    | grep -E '(guest-|prewarm-)' || true
)

if ((${#PODS[@]} == 0)); then
  echo "Burst cluster already clean"
  exit 0
fi

echo "Deleting ${#PODS[@]} Burst worker/prewarm pods from namespace $NAMESPACE"
# Pods can disappear between the listing step and the delete call; that race
# should not invalidate the benchmark run.
kubectl delete -n "$NAMESPACE" "${PODS[@]}" --wait=false --ignore-not-found=true >/dev/null

deadline=$((SECONDS + TIMEOUT_SECONDS))
while ((SECONDS < deadline)); do
  remaining_guest="$(
    kubectl get pods -n "$NAMESPACE" -o name 2>/dev/null \
      | grep "^${POD_PREFIX}" \
      | grep 'guest-' || true
  )"
  if [[ -z "$remaining_guest" ]]; then
    echo "Burst cluster clean"
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for Burst guest pods to disappear" >&2
kubectl get pods -n "$NAMESPACE" -o wide 2>/dev/null | grep 'wskowdev-invoker-' >&2 || true
exit 1
