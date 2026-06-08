#!/usr/bin/env bash

set -euo pipefail

MINIO_URL="${MINIO_URL:-http://127.0.0.1:9000/minio/health/live}"
OW_URL="${OW_URL:-https://127.0.0.1:31001}"

fail=0

echo "Checking burst E2E environment..."

if curl -fsS "$MINIO_URL" >/dev/null 2>&1; then
  echo "  [ok] MinIO is reachable at $MINIO_URL"
else
  echo "  [fail] MinIO is not healthy at $MINIO_URL"
  fail=1
fi

ow_code="$(curl -ksS -o /dev/null -w '%{http_code}' "$OW_URL" || true)"
if [[ "$ow_code" != "000" && -n "$ow_code" ]]; then
  echo "  [ok] OpenWhisk endpoint is reachable at $OW_URL (HTTP $ow_code)"
else
  echo "  [fail] OpenWhisk endpoint is not reachable at $OW_URL"
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo
  echo "Burst E2E prerequisites are not ready."
  echo "Expected local services:"
  echo "  - MinIO on localhost:9000"
  echo "  - OpenWhisk on localhost:31001"
  exit 1
fi

echo
echo "Burst E2E prerequisites look ready."
