#!/usr/bin/env bash
set -euo pipefail

CLOUDLAB_USER="${CLOUDLAB_USER:-sconde}"
CLOUDLAB_HOST="${CLOUDLAB_HOST:-cloudfunctions.urv.cat}"
CLOUDLAB_SSH_KEY="${CLOUDLAB_SSH_KEY:-/home/sergio/.ssh/id_pc1}"
NAMESPACE="${SPARK_SMOKE_NAMESPACE:-spark-sconde-smoke}"

ssh -F /dev/null -i "${CLOUDLAB_SSH_KEY}" "${CLOUDLAB_USER}@${CLOUDLAB_HOST}" "
  set -euo pipefail
  kubectl delete namespace '${NAMESPACE}' --ignore-not-found=true --wait=false
"
