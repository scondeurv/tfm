#!/usr/bin/env bash
set -euo pipefail

PROFILE="minikube"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_ROOT="${SCRIPT_DIR}/cluster_snapshots"
SNAPSHOT_NAME=""
RESTART_AFTER="true"

usage() {
  cat <<'EOF'
Usage: snapshot_burst_cluster.sh [--name NAME] [--output-root DIR] [--leave-stopped] [--profile PROFILE]

Creates a restorable snapshot of the current minikube Docker-driver cluster by saving:
- docker inspect metadata for the minikube node container
- the minikube /var volume contents
- the base kic image used by the node container
- local minikube profile/machine/certs metadata
- a Kubernetes resource export and node image inventory

By default the script stops minikube for a consistent volume snapshot and restarts it
after the backup if it was running before.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      SNAPSHOT_NAME="${2:?missing value for --name}"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="${2:?missing value for --output-root}"
      shift 2
      ;;
    --leave-stopped)
      RESTART_AFTER="false"
      shift
      ;;
    --profile)
      PROFILE="${2:?missing value for --profile}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

for cmd in docker minikube kubectl jq tar; do
  require_cmd "$cmd"
done

if [[ -z "$SNAPSHOT_NAME" ]]; then
  SNAPSHOT_NAME="$(date -u +%Y%m%dT%H%M%SZ)"
fi

SNAPSHOT_DIR="${OUTPUT_ROOT}/${PROFILE}-${SNAPSHOT_NAME}"
mkdir -p "${SNAPSHOT_DIR}"

INSPECT_PATH="${SNAPSHOT_DIR}/minikube-container-inspect.json"
VOLUME_ARCHIVE="${SNAPSHOT_DIR}/minikube-var.tar.gz"
KICBASE_ARCHIVE="${SNAPSHOT_DIR}/kicbase-image.tar"
PROFILE_ARCHIVE="${SNAPSHOT_DIR}/minikube-profile-state.tar.gz"
RESOURCE_EXPORT="${SNAPSHOT_DIR}/k8s-resources.yaml"
EVENT_EXPORT="${SNAPSHOT_DIR}/k8s-events.txt"
NODE_IMAGES="${SNAPSHOT_DIR}/node-images.txt"
SNAPSHOT_META="${SNAPSHOT_DIR}/snapshot-metadata.json"

docker inspect "${PROFILE}" > "${INSPECT_PATH}"

RUNNING_BEFORE="false"
if minikube status -p "${PROFILE}" 2>/dev/null | grep -q "host: Running"; then
  RUNNING_BEFORE="true"
fi

if [[ "${RUNNING_BEFORE}" == "true" ]]; then
  echo "[snapshot] stopping ${PROFILE} for a consistent backup"
  minikube stop -p "${PROFILE}" >/dev/null
fi

echo "[snapshot] exporting kubernetes objects"
kubectl get all,cm,secret,pvc,svc,ingress,job,cronjob,statefulset,deploy,daemonset -A -o yaml > "${RESOURCE_EXPORT}" || true
kubectl get events -A --sort-by=.lastTimestamp > "${EVENT_EXPORT}" || true
minikube ssh -p "${PROFILE}" -- crictl images > "${NODE_IMAGES}" || true

echo "[snapshot] saving minikube /var volume"
VOLUME_NAME="$(jq -r '.[0].Mounts[] | select(.Destination == "/var") | .Name' "${INSPECT_PATH}")"
if [[ -z "${VOLUME_NAME}" || "${VOLUME_NAME}" == "null" ]]; then
  echo "Could not determine minikube volume name from docker inspect" >&2
  exit 1
fi
docker run --rm \
  -v "${VOLUME_NAME}:/from:ro" \
  -v "${SNAPSHOT_DIR}:/backup" \
  busybox:latest \
  sh -c 'cd /from && tar czf /backup/minikube-var.tar.gz .'

echo "[snapshot] saving kic base image"
KIC_IMAGE="$(jq -r '.[0].Config.Image' "${INSPECT_PATH}")"
docker save -o "${KICBASE_ARCHIVE}" "${KIC_IMAGE}"

echo "[snapshot] archiving local ~/.minikube state"
tar czf "${PROFILE_ARCHIVE}" \
  -C /home/sergio \
  ".minikube/profiles/${PROFILE}" \
  ".minikube/machines/${PROFILE}" \
  ".minikube/certs"

jq -n \
  --arg profile "${PROFILE}" \
  --arg snapshot_name "${SNAPSHOT_NAME}" \
  --arg snapshot_dir "${SNAPSHOT_DIR}" \
  --arg created_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg volume_name "${VOLUME_NAME}" \
  --arg kic_image "${KIC_IMAGE}" \
  --arg running_before "${RUNNING_BEFORE}" \
  '{
    profile: $profile,
    snapshot_name: $snapshot_name,
    snapshot_dir: $snapshot_dir,
    created_at: $created_at,
    volume_name: $volume_name,
    kic_image: $kic_image,
    running_before: ($running_before == "true")
  }' > "${SNAPSHOT_META}"

if [[ "${RUNNING_BEFORE}" == "true" && "${RESTART_AFTER}" == "true" ]]; then
  echo "[snapshot] restarting ${PROFILE}"
  minikube start -p "${PROFILE}" >/dev/null
fi

echo "[snapshot] snapshot stored at ${SNAPSHOT_DIR}"
