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

# The correctness suite (oracle + property-based) needs networkx/hypothesis,
# installed only in tests/.venv-test. Prefer it when present.
CORRECTNESS_PY="$TEST_DIR/.venv-test/bin/python"

case "$MODE" in
  all)
    exec "$PYTHON_BIN" -m unittest discover -s "$TEST_DIR" -p 'test_*.py' -v
    ;;
  correctness)
    if [[ ! -x "$CORRECTNESS_PY" ]]; then
      echo "Missing $CORRECTNESS_PY. Create it with:" >&2
      echo "  python3 -m venv $TEST_DIR/.venv-test && $TEST_DIR/.venv-test/bin/pip install -r $TEST_DIR/requirements-test.txt" >&2
      exit 2
    fi
    exec "$CORRECTNESS_PY" -m unittest \
      tests.test_oracle_correctness \
      tests.test_determinism_proof \
      tests.test_cross_backend_correctness -v
    ;;
  *)
    echo "Usage: $0 [all|correctness]" >&2
    exit 2
    ;;
esac
