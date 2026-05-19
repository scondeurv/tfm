#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$ROOT_DIR/tests"

if [[ -x "$ROOT_DIR/labelpropagation/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/labelpropagation/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

MODE="${1:-all}"

case "$MODE" in
  all)
    PATTERN='test_*.py'
    ;;
  *)
    echo "Usage: $0 [all]" >&2
    exit 2
    ;;
esac

exec "$PYTHON_BIN" -m unittest discover -s "$TEST_DIR" -p "$PATTERN" -v
