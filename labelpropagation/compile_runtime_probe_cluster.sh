#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION_DIR="$SCRIPT_DIR/ow-runtime-probe"

if [ -d "$SCRIPT_DIR/burst-communication-middleware" ]; then
    MIDDLEWARE_DIR="$SCRIPT_DIR/burst-communication-middleware"
elif [ -d "$SCRIPT_DIR/../burst-communication-middleware" ]; then
    MIDDLEWARE_DIR="$SCRIPT_DIR/../burst-communication-middleware"
else
    echo "❌ Could not find burst-communication-middleware."
    exit 1
fi

IMAGE="burstcomputing/runtime-rust-burst:latest"

docker run --rm --entrypoint="" \
    -v "$ACTION_DIR":/tmp/input_actions \
    -v "$MIDDLEWARE_DIR":/tmp/input_middleware \
    "$IMAGE" \
    /bin/bash -c "
        set -euo pipefail
        cp -r /tmp/input_actions /tmp/actions_src
        cp -r /tmp/input_middleware /tmp/middleware_src
        rm -rf /usr/src/burst-communication-middleware
        mv /tmp/middleware_src /usr/src/burst-communication-middleware
        python3 /usr/bin/compile.py main /tmp/actions_src /tmp
        cp /tmp/exec /tmp/input_actions/exec_cluster
    "

mkdir -p "$ACTION_DIR/bin"
cp "$ACTION_DIR/exec_cluster" "$ACTION_DIR/bin/exec"
chmod +x "$ACTION_DIR/bin/exec"
rm -f "$ACTION_DIR/exec_cluster"
zip -j "$SCRIPT_DIR/runtime_probe.zip" "$ACTION_DIR/bin/exec"
echo "📦 runtime_probe.zip is ready: $SCRIPT_DIR/runtime_probe.zip"
