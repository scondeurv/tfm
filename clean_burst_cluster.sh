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

# Also sweep pods left in a terminal-but-not-removed state (Evicted / Error /
# Completed) anywhere in the namespace — these accumulate across runs and skew
# `kubectl top` / scheduling without ever being garbage-collected.
mapfile -t DEAD < <(
  kubectl get pods -n "$NAMESPACE" \
    -o 'jsonpath={range .items[?(@.status.phase=="Failed")]}{.metadata.name}{"\n"}{end}{range .items[?(@.status.phase=="Succeeded")]}{.metadata.name}{"\n"}{end}' \
    2>/dev/null | sed '/^$/d' || true
)
if ((${#DEAD[@]} > 0)); then
  echo "Sweeping ${#DEAD[@]} terminal (Failed/Succeeded) pods"
  printf '%s\n' "${DEAD[@]}" \
    | xargs -r kubectl delete pod -n "$NAMESPACE" --wait=false --ignore-not-found=true >/dev/null || true
fi

# Opt-in: purge stuck activations from CouchDB. OFF by default because deleting
# activation docs is destructive; enable only when a crashed run left activations
# wedged "pending" and a fresh campaign must start from a clean ledger.
#   CLEAN_BURST_PURGE_ACTIVATIONS=1 ./clean_burst_cluster.sh
if [[ "${CLEAN_BURST_PURGE_ACTIVATIONS:-0}" == "1" ]]; then
  COUCH=$(kubectl get pods -n "$NAMESPACE" -o name 2>/dev/null \
            | grep -E "${RELEASE_NAME}-couchdb" | head -n1 || true)
  if [[ -n "$COUCH" ]]; then
    echo "Purging stuck (no 'end') activations from CouchDB ($COUCH)"
    # Delete only docs that never recorded an end time (genuinely wedged), via a
    # Mango selector; completed activations are untouched.
    kubectl exec -n "$NAMESPACE" "${COUCH#pod/}" -- sh -c '
      DB="${DB_PREFIX:-test_}activations";
      curl -s "http://$COUCHDB_USER:$COUCHDB_PASSWORD@127.0.0.1:5984/$DB/_find" \
        -H "Content-Type: application/json" \
        -d "{\"selector\":{\"end\":{\"\$exists\":false}},\"fields\":[\"_id\",\"_rev\"],\"limit\":10000}" \
      | sed -e "s/.*\"docs\"://" \
      | grep -o "{\"_id\":\"[^\"]*\",\"_rev\":\"[^\"]*\"}" \
      | while read -r doc; do
          id=$(echo "$doc" | sed -e "s/.*\"_id\":\"//" -e "s/\".*//");
          rev=$(echo "$doc" | sed -e "s/.*\"_rev\":\"//" -e "s/\".*//");
          curl -s -X DELETE "http://$COUCHDB_USER:$COUCHDB_PASSWORD@127.0.0.1:5984/$DB/$id?rev=$rev" >/dev/null;
        done;
      echo "activation purge done"
    ' 2>/dev/null || echo "activation purge skipped (couchdb exec failed)" >&2
  else
    echo "couchdb pod not found; skipping activation purge" >&2
  fi
fi

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
