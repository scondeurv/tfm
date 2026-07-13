#!/usr/bin/env bash
# Provision OpenMPI 4.x + rsmpi build deps on CloudLab compute nodes.
# Idempotent: safe to re-run.
#
# Usage:
#   bash provision_mpi.sh                # provision compute6,compute7
#   bash provision_mpi.sh compute6 compute7 compute5
#
# Requires passwordless sudo and ssh access to each target host.
#
# Why we pin OpenMPI 4.x: rsmpi 0.7/0.8 reads `MPI_SOURCE` / `MPI_TAG` directly
# from `MPI_Status`, which OpenMPI 5.x removed (moved to opaque `_address`).
# CloudLab nodes ship Ubuntu 22.04 → apt's `openmpi-bin` is 4.1.2, which is
# the supported pairing. Do NOT upgrade to 5.x here.

set -euo pipefail

HOSTS=("$@")
if [ "${#HOSTS[@]}" -eq 0 ]; then
    HOSTS=(compute6 compute7)
fi

REQUIRED_PKGS=(
    openmpi-bin
    libopenmpi-dev
    libclang-14-dev      # rsmpi's bindgen
    pkg-config
    build-essential
)

provision_host() {
    local host="$1"
    echo "==> Provisioning ${host}"

    ssh -o StrictHostKeyChecking=no -o BatchMode=yes "${host}" \
        "sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq && \
         sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ${REQUIRED_PKGS[*]}"

    # Verify versions
    local mpirun_version
    mpirun_version=$(ssh -o BatchMode=yes "${host}" "mpirun --version 2>&1 | head -1")
    echo "    ${host}: ${mpirun_version}"

    case "${mpirun_version}" in
        *"Open MPI"*"5."*)
            echo "    ERROR: ${host} has OpenMPI 5.x; rsmpi requires 4.x" >&2
            return 1
            ;;
        *"Open MPI"*"4."*)
            ;;
        *)
            echo "    WARNING: ${host} has unexpected MPI build: ${mpirun_version}" >&2
            ;;
    esac
}

verify_interconnect() {
    # Smoke test: launch /bin/hostname on each rank across all hosts. If MPI
    # cannot reach a host, mpirun will exit non-zero here.
    local hostlist
    hostlist=$(IFS=,; echo "${HOSTS[*]}")
    echo "==> Verifying mpirun interconnect across ${hostlist}"
    ssh -o BatchMode=yes "${HOSTS[0]}" \
        "mpirun -np ${#HOSTS[@]} -H ${hostlist} hostname" \
        || { echo "    ERROR: mpirun cross-host test failed" >&2; return 1; }
}

for host in "${HOSTS[@]}"; do
    provision_host "${host}"
done

verify_interconnect

echo "==> Done. OpenMPI ready on: ${HOSTS[*]}"
