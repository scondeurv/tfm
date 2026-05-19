#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: restore_burst_cluster.sh <snapshot-dir> [--profile PROFILE]" >&2
  exit 1
fi

SNAPSHOT_DIR="$1"
shift
PROFILE="minikube"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:?missing value for --profile}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: restore_burst_cluster.sh <snapshot-dir> [--profile PROFILE]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

require_file() {
  [[ -f "$1" ]] || {
    echo "Missing required file: $1" >&2
    exit 1
  }
}

for cmd in docker minikube kubectl jq tar; do
  command -v "$cmd" >/dev/null 2>&1 || {
    echo "Missing required command: $cmd" >&2
    exit 1
  }
done

INSPECT_PATH="${SNAPSHOT_DIR}/minikube-container-inspect.json"
VOLUME_ARCHIVE="${SNAPSHOT_DIR}/minikube-var.tar.gz"
KICBASE_ARCHIVE="${SNAPSHOT_DIR}/kicbase-image.tar"
PROFILE_ARCHIVE="${SNAPSHOT_DIR}/minikube-profile-state.tar.gz"

require_file "${INSPECT_PATH}"
require_file "${VOLUME_ARCHIVE}"
require_file "${KICBASE_ARCHIVE}"
require_file "${PROFILE_ARCHIVE}"

CONTAINER_NAME="$(jq -r '.[0].Name | ltrimstr("/")' "${INSPECT_PATH}")"
HOSTNAME_VALUE="$(jq -r '.[0].Config.Hostname' "${INSPECT_PATH}")"
NETWORK_NAME="$(jq -r '.[0].HostConfig.NetworkMode' "${INSPECT_PATH}")"
IP_ADDRESS="$(jq -r '.[0].NetworkSettings.Networks | to_entries[0].value.IPAddress' "${INSPECT_PATH}")"
GATEWAY="$(jq -r '.[0].NetworkSettings.Networks | to_entries[0].value.Gateway' "${INSPECT_PATH}")"
PREFIX_LEN="$(jq -r '.[0].NetworkSettings.Networks | to_entries[0].value.IPPrefixLen' "${INSPECT_PATH}")"
MEMORY_BYTES="$(jq -r '.[0].HostConfig.Memory' "${INSPECT_PATH}")"
ENTRYPOINT="$(jq -r '.[0].Path' "${INSPECT_PATH}")"
IMAGE_REF="$(jq -r '.[0].Config.Image' "${INSPECT_PATH}")"
VOLUME_NAME="$(jq -r '.[0].Mounts[] | select(.Destination == "/var") | .Name' "${INSPECT_PATH}")"

network_subnet() {
  local ip="$1"
  local prefix="$2"
  if [[ "${prefix}" != "24" ]]; then
    echo "Unsupported prefix length: ${prefix}" >&2
    exit 1
  fi
  IFS='.' read -r a b c _ <<< "${ip}"
  echo "${a}.${b}.${c}.0/${prefix}"
}

mapfile -t PORT_ARGS < <(
  jq -r '
    .[0].NetworkSettings.Ports
    | to_entries[]
    | .key as $container_port
    | (.value // [])[]
    | select(.HostPort != null and .HostPort != "")
    | "--publish=" + ((if .HostIp == "" then "" else .HostIp + ":" end) + .HostPort + ":" + ($container_port | split("/")[0]) + "/" + ($container_port | split("/")[1]))
  ' "${INSPECT_PATH}"
)

mapfile -t BIND_ARGS < <(jq -r '.[0].HostConfig.Binds[] | "--volume=" + .' "${INSPECT_PATH}")
mapfile -t TMPFS_ARGS < <(jq -r '.[0].HostConfig.Tmpfs | to_entries[] | "--tmpfs=" + .key + ":" + .value' "${INSPECT_PATH}")
mapfile -t SECOPT_ARGS < <(jq -r '.[0].HostConfig.SecurityOpt[] | "--security-opt=" + .' "${INSPECT_PATH}")
mapfile -t ENV_ARGS < <(jq -r '.[0].Config.Env[] | "--env=" + .' "${INSPECT_PATH}")
mapfile -t LABEL_ARGS < <(jq -r '.[0].Config.Labels | to_entries[] | "--label=" + .key + "=" + .value' "${INSPECT_PATH}")
mapfile -t CMD_ARGS < <(jq -r '.[0].Args[]?' "${INSPECT_PATH}")

if minikube status -p "${PROFILE}" 2>/dev/null | grep -q "host: Running"; then
  echo "[restore] stopping ${PROFILE}"
  minikube stop -p "${PROFILE}" >/dev/null || true
fi

echo "[restore] removing existing container and volume"
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
docker volume rm "${VOLUME_NAME}" >/dev/null 2>&1 || true

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  echo "[restore] creating docker network ${NETWORK_NAME}"
  docker network create \
    --driver bridge \
    --subnet "$(network_subnet "${IP_ADDRESS}" "${PREFIX_LEN}")" \
    --gateway "${GATEWAY}" \
    "${NETWORK_NAME}" >/dev/null
fi

echo "[restore] loading base image"
docker load -i "${KICBASE_ARCHIVE}" >/dev/null

echo "[restore] recreating named volume ${VOLUME_NAME}"
docker volume create "${VOLUME_NAME}" >/dev/null
docker run --rm \
  -v "${VOLUME_NAME}:/to" \
  -v "${SNAPSHOT_DIR}:/backup:ro" \
  busybox:latest \
  sh -c 'cd /to && tar xzf /backup/minikube-var.tar.gz'

echo "[restore] restoring ~/.minikube profile state"
mkdir -p /home/sergio/.minikube
tar xzf "${PROFILE_ARCHIVE}" -C /home/sergio

echo "[restore] recreating minikube node container"
docker run -d \
  --name "${CONTAINER_NAME}" \
  --hostname "${HOSTNAME_VALUE}" \
  --privileged \
  --tty \
  --memory "${MEMORY_BYTES}" \
  --network "${NETWORK_NAME}" \
  --ip "${IP_ADDRESS}" \
  "${PORT_ARGS[@]}" \
  "${BIND_ARGS[@]}" \
  "${TMPFS_ARGS[@]}" \
  "${SECOPT_ARGS[@]}" \
  "${ENV_ARGS[@]}" \
  "${LABEL_ARGS[@]}" \
  --entrypoint "${ENTRYPOINT}" \
  "${IMAGE_REF}" \
  "${CMD_ARGS[@]}" >/dev/null

echo "[restore] updating minikube context"
minikube update-context -p "${PROFILE}" >/dev/null || true

echo "[restore] waiting for Kubernetes API"
for _ in $(seq 1 60); do
  if kubectl get nodes >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

kubectl get nodes
echo "[restore] restore completed from ${SNAPSHOT_DIR}"
