#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIDDLEWARE_DIR="${ROOT_DIR}/burst-communication-middleware"

ensure_middleware_link() {
  local algo_dir="$1"
  local link_path="${ROOT_DIR}/${algo_dir}/burst-communication-middleware"

  if [[ -e "${link_path}" ]]; then
    return
  fi

  ln -s ../burst-communication-middleware "${link_path}"
}

run_cargo_tests() {
  local name="$1"
  local crate_dir="$2"

  printf '\n== %s ==\n' "${name}"
  cargo test --offline --manifest-path "${ROOT_DIR}/${crate_dir}/Cargo.toml"
}

if [[ ! -d "${MIDDLEWARE_DIR}" ]]; then
  echo "Missing ${MIDDLEWARE_DIR}" >&2
  exit 1
fi

ensure_middleware_link "bfs"
ensure_middleware_link "sssp"
ensure_middleware_link "labelpropagation"

run_cargo_tests "BFS Burst core" "bfs/ow-bfs"
run_cargo_tests "SSSP Burst core" "sssp/ow-sssp"
run_cargo_tests "Label Propagation Burst core" "labelpropagation/ow-lp"
