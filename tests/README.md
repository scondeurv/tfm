# Deterministic E2E Tests

This directory contains deterministic end-to-end regression tests for the graph algorithms used in the TFM work.

## Scope

The package includes self-contained unit/regression tests for the campaign
algorithms and shared metrics helpers. Tests that required standalone crates,
external validators, MinIO, or OpenWhisk are intentionally not included in the
campaign source zip.

## Covered algorithms

- `BFS`
- `SSSP`
- `Label Propagation`

## Commands

From the package root:

```bash
make test-e2e
```

Equivalent direct wrapper:

```bash
./run_e2e_tests.sh all
```

## CI-friendly target

For automation or lightweight local CI, use:

```bash
make ci
```

This runs the package-local test suite without requiring MinIO, OpenWhisk, or
standalone binaries.

## Algorithm Correctness

To validate the Rust Burst algorithm cores against their deterministic unit and
distributed in-process tests, run from the package root:

```bash
./run_algorithm_correctness_tests.sh
```

This uses `cargo test --offline` for `bfs/ow-bfs`, `sssp/ow-sssp`, and
`labelpropagation/ow-lp`. It does not run as part of the campaign measurement
path.

## Fixtures

Fixtures live in `tests/fixtures/` and are intentionally tiny and explicit, so expected outputs are easy to audit.
