"""Independent correctness oracle for the graph algorithms.

The cross-backend suite proves *equivalence* (all backends agree with the
standalone reference). This module closes the remaining gap — *correctness of the
reference itself* — by computing the expected result with an independent,
well-trusted library (networkx), so a bug shared by every backend (i.e. living in
the standalone algorithm) is still caught.

Semantics aligned with the Rust CSR implementations:
- Graphs are directed, 0-indexed, node set is exactly {0..n-1} (isolated nodes
  included via add_nodes_from).
- **MultiDiGraph** is used so parallel edges and self-loops count with their
  multiplicity, exactly as the Rust CSR does (out_degree counts duplicates;
  PageRank shares are sent per CSR entry). A plain DiGraph would collapse
  multi-edges and disagree on the self_loops_and_multi fixture.
- Unreachable BFS level = U32_MAX (matches Rust UNVISITED); unreachable SSSP
  distance = +inf (matches f32::INFINITY).

The oracle returns full result vectors (index = node id) so the existing
comparators in determinism_compare.py can be reused.
"""
from __future__ import annotations

import math
from typing import Iterable

import networkx as nx

U32_MAX = 2**32 - 1  # Rust UNVISITED / UNKNOWN sentinel


def _multidigraph(edges: Iterable[tuple[int, int]], n: int) -> "nx.MultiDiGraph":
    g = nx.MultiDiGraph()
    g.add_nodes_from(range(n))
    g.add_edges_from((int(u), int(v)) for u, v in edges)
    return g


# ----------------------------------------------------------------------------
# BFS — unweighted shortest-path hop count from source.
# ----------------------------------------------------------------------------
def bfs_levels(edges: Iterable[tuple[int, int]], n: int, source: int = 0) -> list[int]:
    g = _multidigraph(edges, n)
    dist = nx.single_source_shortest_path_length(g, source)
    return [int(dist.get(i, U32_MAX)) for i in range(n)]


# ----------------------------------------------------------------------------
# SSSP — non-negative weighted shortest path (Dijkstra) from source.
# Weights come from the same deterministic formula used by the fixtures
# (edges_to_tsv_weighted): w_uv = 1.0 + ((u*31 + v*17 + seed) % 13).
# ----------------------------------------------------------------------------
def sssp_weight(u: int, v: int, seed: int = 7) -> float:
    return 1.0 + float((u * 31 + v * 17 + seed) % 13)


def weighted_edges_from_unweighted(
    edges: Iterable[tuple[int, int]], seed: int = 7
) -> list[tuple[int, int, float]]:
    return [(u, v, sssp_weight(u, v, seed)) for u, v in edges]


def sssp_distances(
    weighted_edges: Iterable[tuple[int, int, float]], n: int, source: int = 0
) -> list[float]:
    g = nx.MultiDiGraph()
    g.add_nodes_from(range(n))
    for u, v, w in weighted_edges:
        g.add_edge(int(u), int(v), weight=float(w))
    dist = nx.single_source_dijkstra_path_length(g, source, weight="weight")
    return [float(dist.get(i, math.inf)) for i in range(n)]


# ----------------------------------------------------------------------------
# PageRank — power iteration; networkx handles dangling nodes by uniform
# redistribution, matching the Rust implementation (dangling_mass / n).
# Returns ranks normalized to sum 1.0, index = node id.
# ----------------------------------------------------------------------------
def pagerank(
    edges: Iterable[tuple[int, int]],
    n: int,
    damping: float = 0.85,
    max_iter: int = 1000,
    tol: float = 1e-10,
) -> list[float]:
    g = _multidigraph(edges, n)
    pr = nx.pagerank(g, alpha=damping, max_iter=max_iter, tol=tol)
    return [float(pr.get(i, 0.0)) for i in range(n)]


# ----------------------------------------------------------------------------
# Label Propagation — no exact oracle (semi-supervised LP with seeds is not a
# standard networkx algorithm; tie-break is implementation-defined). Instead we
# verify the invariants that any correct semi-supervised LP fixed point must
# satisfy. Tie-break rule of the repo: highest neighbour-label frequency, ties
# broken towards the SMALLEST label id (verified in lpst/src/lib.rs:100-155).
# ----------------------------------------------------------------------------
def _majority_label(neighbor_labels: list[int], current: int) -> int:
    """Repo rule: most frequent label, ties -> smallest id. `current` returned
    when there are no labeled neighbours."""
    if not neighbor_labels:
        return current
    counts: dict[int, int] = {}
    for l in neighbor_labels:
        counts[l] = counts.get(l, 0) + 1
    best = current
    best_count = 0
    for label in sorted(counts):  # ascending => ties resolve to smallest
        c = counts[label]
        if c > best_count:
            best = label
            best_count = c
    return best


def lp_invariants(
    edges: Iterable[tuple[int, int]],
    n: int,
    labels: list[int],
    seeds: dict[int, int],
    *,
    unknown: int = U32_MAX,
) -> tuple[bool, str]:
    """Check the invariants a converged semi-supervised LP must satisfy.

    Returns (ok, diagnostic). Invariants:
    1. Seeds preserved: labels[node] == seed for every seeded node.
    2. Coverage: every node with >= 1 labeled (non-UNKNOWN) neighbour is itself
       labeled (isolated/unseeded nodes legitimately stay UNKNOWN).
    3. Fixed point: every non-seed labeled node equals the majority of its
       labeled out-neighbours (ties -> smallest), i.e. no update would change it.
    """
    if len(labels) != n:
        return False, f"labels length {len(labels)} != n {n}"

    out_neighbors: dict[int, list[int]] = {i: [] for i in range(n)}
    for u, v in edges:
        out_neighbors[int(u)].append(int(v))

    for node, seed in seeds.items():
        if labels[node] != seed:
            return False, f"seed violation: node {node} label {labels[node]} != seed {seed}"

    for i in range(n):
        labeled_nbrs = [labels[v] for v in out_neighbors[i] if labels[v] != unknown]
        if i in seeds:
            continue
        if labels[i] == unknown:
            if labeled_nbrs:
                return False, f"coverage: node {i} UNKNOWN but has labeled neighbours {labeled_nbrs[:5]}"
            continue
        expected = _majority_label(labeled_nbrs, labels[i])
        if expected != labels[i]:
            return False, (
                f"not a fixed point: node {i} label {labels[i]} but majority of "
                f"neighbours is {expected}"
            )
    return True, "ok"
