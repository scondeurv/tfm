# SSSP (Single Source Shortest Path) — TFM Agent Instructions

## Algorithm Overview

**SSSP (Bellman-Ford variants)** computes the shortest distances from a single
source node to all other nodes in a weighted directed graph with non-negative
edge weights.

- **Standalone**: Synchronous Bellman-Ford relaxation on a full local graph
  (bounded by `MAX_ITERATIONS`)
- **Distributed (Burst)**: Iterative Bellman-Ford relaxation with reduce/broadcast
  convergence — each iteration O(E/P) per worker, O(V) messages

## Architecture

```
sssp/
├── sssp-standalone/           # Rust standalone binary (Bellman-Ford)
│   ├── src/
│   │   ├── lib.rs             # Core algorithm: run_bellman_ford() + 13 unit tests
│   │   ├── main.rs            # CLI binary: <graph_file> <num_nodes> [source_node]
│   │   └── bin/
│   │       └── generate_graph.rs  # Weighted random graph generator
│   └── Cargo.toml
│
├── ow-sssp/                   # OpenWhisk burst worker (Bellman-Ford)
│   ├── src/
│   │   ├── lib.rs             # Distributed relaxation + DistanceMessage + 9 tests
│   │   └── testing.rs         # Local Redis testing binary
│   └── Cargo.toml
│
├── setup_large_sssp_data.py   # Graph generation (local + S3 partitions)
├── sssp_utils.py              # Payload generation + CLI argument helpers
├── sssp.py                    # Direct burst launcher (single run)
├── benchmark_sssp.py          # Standalone vs Burst benchmark
├── validate_crossover_sssp.py # Multi-run crossover validation (RUNS=5)
├── quick_crossover_sssp.py    # 2-point interpolation + 4-subplot figure
├── plot_sssp_results.py       # 3×2 comprehensive + crossover plots
├── compile_sssp_cluster.sh    # Docker cross-compile → sssp.zip
├── pyproject.toml
├── requirements.txt
└── ow_client/                 # Shared OpenWhisk client library (symlink/copy)
```

## Key Design Decisions

### DistanceMessage Encoding
- `DistanceMessage(Vec<u32>)` where slots `[0..N)` encode f32 distances as
  `u32` bits (`f32::to_bits()`), and slot `[N]` is the change counter.
- **Key insight**: For non-negative f32 values, their bit patterns as u32 sort
  in the same order. This means `u32::min(a.to_bits(), b.to_bits())` produces
  the correct `f32::min(a, b)` without decode/encode, enabling efficient
  element-wise reduce.

### Bellman-Ford Variants (Standalone vs Distributed)
- Both implementations use Bellman-Ford style relaxation, but execution models differ:
  standalone relaxes over a local full graph; burst relaxes per partition and then
  synchronizes via global reduce/broadcast each iteration.
- Worst case: O(V) iterations for chain graphs; typical random graphs
  converge in O(diameter) ≈ O(log N) iterations.

### Reduce Semantics
- Unlike BFS/LP where nodes are disjoint across workers (first-non-sentinel wins),
  in SSSP multiple workers can discover shorter paths to the same destination node.
- Reduce must be **element-wise minimum** (`u32::min` on bits), not first-non-sentinel.
- The change counter (last slot) is summed to detect global convergence.

### Float Comparison Caveat
- Even with Bellman-Ford in both modes, floating-point reductions can differ slightly
  due to partitioned execution and reduction order. Keep tolerance-based validation
  for `max_distance`.

## Data Format

- **TSV**: `src\tdst\tweight` (weight defaults to 1.0 if missing)
- **S3 partitioning**: `src % num_partitions` — same scheme as BFS/LP/Louvain
- **Sentinel**: `f32::INFINITY` (`0x7F800000` as u32 bits) for unreachable nodes

## Benchmark Flow

1. `setup_large_sssp_data.py` — Generate weighted graph + upload to MinIO
2. `compile_sssp_cluster.sh` — Cross-compile ow-sssp → sssp.zip
3. `benchmark_sssp.py` — Run standalone + burst, print timing lines
4. `validate_crossover_sssp.py` — Multi-size multi-run crossover sweep
5. `plot_sssp_results.py` — Generate analysis figures

## Key Output Lines (parsed by crossover scripts)

```
SSSP Standalone Processing Time (Execution): X ms
SSSP Burst Processing Time (Distributed Span): X ms
```

## Validation

- **reachable_nodes**: Must match exactly between standalone and burst
- **max_distance**: Float comparison with tolerance (1e-4 relative, 1e-3 absolute)
  because distributed reduction order and floating-point accumulation can differ slightly
  from standalone evaluation, while both should converge to equivalent shortest paths

## Building

```bash
# Standalone
cd sssp-standalone && cargo build --release && cargo test

# Distributed worker (check)
cd ow-sssp && cargo check && cargo test

# Cross-compile for cluster
./compile_sssp_cluster.sh
```
