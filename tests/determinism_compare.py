"""Vector comparators for cross-backend determinism proofs.

Each comparator takes two JSON records (`ref`, `cmp`) and the topology
metadata, and either returns None on agreement or raises AssertionError with
a concrete diff.

Why per-algo:
- BFS levels: u32. Unreachable = u32::MAX (0xFFFF_FFFF). Exact equality.
- SSSP distances: f32. Unreachable = f32::INFINITY. Bit-exact equality
  (CSR traversal order identical → same fp ops in same order).
- PageRank ranks: f32. Reduction order varies across rayon thread count and
  MPI rank count → element-wise ε comparison (default 1e-5 absolute).
- LP labels: arbitrary integer IDs. Compare via equivalence classes — same
  partition iff f(label_ref[i]) == label_cmp[i] for some bijection f.
"""
from __future__ import annotations

from collections import defaultdict


def assert_bfs_levels_equal(ref: dict, cmp: dict, label: str) -> None:
    rl = ref["levels"]
    cl = cmp["levels"]
    assert len(rl) == len(cl), f"{label}: levels length {len(rl)} vs {len(cl)}"
    for i, (a, b) in enumerate(zip(rl, cl)):
        assert a == b, f"{label}: levels[{i}] {a} != {b}"


def assert_sssp_distances_equal(ref: dict, cmp: dict, label: str) -> None:
    rd = ref["distances"]
    cd = cmp["distances"]
    assert len(rd) == len(cd), f"{label}: distances length {len(rd)} vs {len(cd)}"
    for i, (a, b) in enumerate(zip(rd, cd)):
        if a != b:
            raise AssertionError(f"{label}: distances[{i}] {a!r} != {b!r} (bit-exact required)")


def assert_pagerank_ranks_close(ref: dict, cmp: dict, label: str, eps: float = 1e-5) -> None:
    rr = ref["rank"]
    cr = cmp["rank"]
    assert len(rr) == len(cr), f"{label}: rank length {len(rr)} vs {len(cr)}"
    max_diff = 0.0
    arg_max = -1
    for i, (a, b) in enumerate(zip(rr, cr)):
        d = abs(float(a) - float(b))
        if d > max_diff:
            max_diff = d
            arg_max = i
    assert max_diff <= eps, (
        f"{label}: PR rank diverges: max |Δ|={max_diff:.3e} at idx {arg_max} "
        f"(ref={rr[arg_max]} cmp={cr[arg_max]}, eps={eps:.0e})"
    )


def assert_lp_partition_equivalent(ref: dict, cmp: dict, label: str) -> None:
    """Two label vectors describe the same partition iff there is a bijection
    f such that f(ref[i]) == cmp[i] for all i. Algorithm:

    1. Build map ref_label → set of indices.
    2. Build map cmp_label → set of indices.
    3. Two partitions agree iff the two collections of index-sets are equal
       (as sets of frozensets).
    """
    rl = ref["labels"]
    cl = cmp["labels"]
    assert len(rl) == len(cl), f"{label}: labels length {len(rl)} vs {len(cl)}"

    def partition(lbls: list[int]) -> set[frozenset[int]]:
        groups: dict[int, list[int]] = defaultdict(list)
        for i, l in enumerate(lbls):
            groups[l].append(i)
        return {frozenset(g) for g in groups.values()}

    p_ref = partition(rl)
    p_cmp = partition(cl)
    if p_ref != p_cmp:
        only_ref = p_ref - p_cmp
        only_cmp = p_cmp - p_ref
        raise AssertionError(
            f"{label}: LP partitions differ. "
            f"Groups only in ref: {sorted(map(sorted, only_ref))[:3]}... "
            f"only in cmp: {sorted(map(sorted, only_cmp))[:3]}..."
        )
