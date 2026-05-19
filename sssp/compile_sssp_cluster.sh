#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION_DIR="$SCRIPT_DIR/ow-sssp"

if [ -d "$SCRIPT_DIR/burst-communication-middleware" ]; then
    MIDDLEWARE_DIR="$SCRIPT_DIR/burst-communication-middleware"
elif [ -d "$SCRIPT_DIR/../burst-communication-middleware" ]; then
    MIDDLEWARE_DIR="$SCRIPT_DIR/../burst-communication-middleware"
else
    echo "❌ Could not find burst-communication-middleware."
    echo "   Checked:"
    echo "   - $SCRIPT_DIR/burst-communication-middleware"
    echo "   - $SCRIPT_DIR/../burst-communication-middleware"
    exit 1
fi

IMAGE="burstcomputing/runtime-rust-burst:latest"

if ! command -v docker >/dev/null 2>&1; then
    echo "❌ docker is not installed or not in PATH."
    exit 1
fi

if [ ! -d "$ACTION_DIR" ]; then
    echo "❌ Missing action directory: $ACTION_DIR"
    exit 1
fi

echo "🚀 Starting compilation of SSSP action using cluster-identical environment..."
echo "   Action dir:      $ACTION_DIR"
echo "   Middleware dir:  $MIDDLEWARE_DIR"

docker run --rm --entrypoint="" \
    -v "$ACTION_DIR":/tmp/input_actions \
    -v "$MIDDLEWARE_DIR":/tmp/input_middleware \
    "$IMAGE" \
    /bin/bash -c "
        set -euo pipefail

        # 1. Prepare isolated source folders (avoiding mount point busy errors)
        cp -r /tmp/input_actions /tmp/actions_src
        cp -r /tmp/input_middleware /tmp/middleware_src

        # 2. Use the local middleware checkout so cluster builds include
        #    Redis/backend fixes from this workspace.
        rm -rf /usr/src/burst-communication-middleware
        mv /tmp/middleware_src /usr/src/burst-communication-middleware

        # 3. Compile using the image's internal script
        python3 /usr/bin/compile.py main /tmp/actions_src /tmp

        # 4. Copy the resulting binary back to the mount
        if [ ! -f /tmp/exec ]; then
            echo '❌ compile.py finished without producing /tmp/exec'
            exit 1
        fi
        cp /tmp/exec /tmp/input_actions/exec_cluster
    "

echo "✅ Compilation successful!"
if [ ! -f "$ACTION_DIR/exec_cluster" ]; then
    echo "❌ Missing compiled binary: $ACTION_DIR/exec_cluster"
    exit 1
fi
TMP_PACKAGE_DIR="$(mktemp -d)"
cp "$ACTION_DIR/exec_cluster" "$TMP_PACKAGE_DIR/exec"
chmod +x "$TMP_PACKAGE_DIR/exec"
rm -f "$ACTION_DIR/exec_cluster"
zip -j "$SCRIPT_DIR/sssp.zip" "$TMP_PACKAGE_DIR/exec"
rm -rf "$TMP_PACKAGE_DIR"
echo "📦 sssp.zip is ready: $SCRIPT_DIR/sssp.zip"
