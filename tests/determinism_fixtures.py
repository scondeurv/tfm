"""Deterministic graph fixtures for cross-backend determinism proofs.

Each generator returns a tuple (edges, num_nodes) where:
- `edges` is a list of (src, dst) or (src, dst, weight) tuples (weight only for
  SSSP). Reproducible across Python versions: fixed-seed RNGs, sorted output.
- `num_nodes` is the canonical node count (max id + 1, contiguous IDs).

The same fixtures are consumed by:
- tests/test_cross_backend_correctness.py (local: standalone + rayon + mpi)
- campaigns/validate_implementations_cluster.sh (cluster: MPI + Burst + Spark)

PR + LP variants use the unweighted edges. SSSP appends f32 weights deterministically.
BFS variants need an explicit source vertex; convention: source = 0.
"""
from __future__ import annotations

import random
from typing import Iterable

# ----------------------------------------------------------------------------
# Topology generators (unweighted). All deterministic, sorted.
# ----------------------------------------------------------------------------


def path_graph(n: int = 100) -> tuple[list[tuple[int, int]], int]:
    """Simple chain 0→1→2→...→n-1. Worst-case BFS/SSSP depth = n-1."""
    return [(i, i + 1) for i in range(n - 1)], n


def star_with_sinks(n: int = 50) -> tuple[list[tuple[int, int]], int]:
    """Hub 0 with `n-1` sinks (no out-edges). PR-relevant: dangling vertices.

    Edges: 0 → i for i in 1..n-1. Sinks 1..n-1 have no out-edges.
    """
    return [(0, i) for i in range(1, n)], n


def two_components(n_each: int = 50) -> tuple[list[tuple[int, int]], int]:
    """Two disjoint paths. BFS/SSSP from source=0 must mark second component
    as unreachable (u32::MAX or f32::INF).
    """
    n = 2 * n_each
    a = [(i, i + 1) for i in range(n_each - 1)]
    b = [(n_each + i, n_each + i + 1) for i in range(n_each - 1)]
    return a + b, n


def er_random(n: int = 1000, p: float = 0.01, seed: int = 42) -> tuple[list[tuple[int, int]], int]:
    """Erdős–Rényi G(n, p), directed, no self-loops. Seed fixed for reproducibility.

    Expected E ≈ n*(n-1)*p. At n=1000, p=0.01 → ~10k edges. Mixes reachable +
    likely-unreachable vertices, breaks degenerate-topology assumptions.
    """
    rng = random.Random(seed)
    edges: list[tuple[int, int]] = []
    for u in range(n):
        for v in range(n):
            if u == v:
                continue
            if rng.random() < p:
                edges.append((u, v))
    return edges, n


def self_loops_and_multi(n: int = 20) -> tuple[list[tuple[int, int]], int]:
    """Pathological fixture: every node has a self-loop, several edges duplicated.

    Tests that backends agree under degenerate input — self-loops must not
    affect BFS levels, and multi-edges must not affect PR/LP (they are summed
    via CSR semantics, so both sides see the same multiplicity).
    """
    edges: list[tuple[int, int]] = []
    for i in range(n):
        edges.append((i, i))
    for i in range(n - 1):
        edges.append((i, i + 1))
        edges.append((i, i + 1))
    edges.append((0, n // 2))
    edges.append((0, n // 2))
    return edges, n


# ----------------------------------------------------------------------------
# TSV serializers.
# ----------------------------------------------------------------------------


def edges_to_tsv_unweighted(edges: Iterable[tuple[int, int]]) -> str:
    return "".join(f"{u}\t{v}\n" for u, v in edges)


def edges_to_tsv_weighted(edges: Iterable[tuple[int, int]], seed: int = 7) -> str:
    """Deterministic weights for SSSP: w_uv = 1.0 + ((u * 31 + v * 17 + seed) % 13)."""
    out: list[str] = []
    for u, v in edges:
        w = 1.0 + float((u * 31 + v * 17 + seed) % 13)
        out.append(f"{u}\t{v}\t{w}\n")
    return "".join(out)


FIXTURES: dict[str, callable] = {
    "path_100": lambda: path_graph(100),
    "star_50": lambda: star_with_sinks(50),
    "two_components_50each": lambda: two_components(50),
    "er_1000_p01": lambda: er_random(1000, 0.01, seed=42),
    "self_loops_20": lambda: self_loops_and_multi(20),
}
