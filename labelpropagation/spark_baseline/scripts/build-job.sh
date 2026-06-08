#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JOB_DIR="$ROOT_DIR/jobs"

cd "$JOB_DIR"

if command -v sbt >/dev/null 2>&1; then
  sbt clean package
  exit 0
fi

if [[ -n "${SBT_IMAGE:-}" ]]; then
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "$JOB_DIR:/workspace" \
    -w /workspace \
    "$SBT_IMAGE" \
    sbt clean package
  exit 0
fi

echo "sbt not found in host." >&2
echo "Install sbt or set SBT_IMAGE to an sbtscala/scala-sbt image." >&2
exit 1
