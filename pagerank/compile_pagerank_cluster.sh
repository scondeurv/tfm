#!/bin/bash
# Build the ow-pagerank Burst action binary using the cluster-identical Rust
# image, then package as pagerank.zip ready for OpenWhisk upload.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION_DIR="$SCRIPT_DIR/ow-pagerank"

# Prefer the LP-tree copy of the middleware (canonical location) since
# pagerank/ has no sibling copy of its own.
if [ -d "$SCRIPT_DIR/burst-communication-middleware" ]; then
    MIDDLEWARE_DIR="$(cd "$SCRIPT_DIR/burst-communication-middleware" && pwd -P)"
elif [ -d "$SCRIPT_DIR/../labelpropagation/burst-communication-middleware" ]; then
    MIDDLEWARE_DIR="$(cd "$SCRIPT_DIR/../labelpropagation/burst-communication-middleware" && pwd -P)"
else
    echo "❌ Could not find burst-communication-middleware sibling of pagerank/"
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

echo "🚀 Compiling PageRank action with cluster-identical environment..."
echo "   Action dir:      $ACTION_DIR"
echo "   Middleware dir:  $MIDDLEWARE_DIR"

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
        if [ ! -f /tmp/exec ]; then
            echo '❌ compile.py finished without producing /tmp/exec'
            exit 1
        fi
        cp /tmp/exec /tmp/input_actions/exec_cluster
    "

echo "✅ Compilation successful!"
mkdir -p "$ACTION_DIR/bin"
cp "$ACTION_DIR/exec_cluster" "$ACTION_DIR/bin/exec"
chmod +x "$ACTION_DIR/bin/exec"
rm -f "$ACTION_DIR/exec_cluster"
zip -j "$SCRIPT_DIR/pagerank.zip" "$ACTION_DIR/bin/exec"
echo "📦 Zip is ready: $SCRIPT_DIR/pagerank.zip"
