"""Hand-computed known answers — the only truth source that is independent of
BOTH the implementation and networkx. Used to (a) pin the reference binary and
(b) sanity-check the oracle itself.

Each entry is fully worked out by hand. SSSP weights follow the repo formula
w(u,v) = 1.0 + ((u*31 + v*17 + 7) % 13)  (see oracle.sssp_weight).
BFS unreachable = U32_MAX. LP tie-break = highest frequency, ties -> smallest id.
"""
from __future__ import annotations

U32_MAX = 2**32 - 1

# ---- BFS known answers: (edges, n, source, expected_levels) ----------------
BFS_CASES = {
    # 0->1->2->3->4 chain
    "path_5": ([(0, 1), (1, 2), (2, 3), (3, 4)], 5, 0, [0, 1, 2, 3, 4]),
    # documented 5-node DAG: 0->1,0->2,1->3,2->3,3->4
    "tri_chain_5": ([(0, 1), (0, 2), (1, 3), (2, 3), (3, 4)], 5, 0, [0, 1, 1, 2, 3]),
    # star hub 0 -> {1,2,3}
    "star_4": ([(0, 1), (0, 2), (0, 3)], 4, 0, [0, 1, 1, 1]),
    # directed 3-cycle 0->1->2->0
    "cycle_3": ([(0, 1), (1, 2), (2, 0)], 3, 0, [0, 1, 2]),
    # two disjoint edges: 0->1 reachable, 2->3 unreachable from 0
    "two_edges": ([(0, 1), (2, 3)], 4, 0, [0, 1, U32_MAX, U32_MAX]),
}

# ---- SSSP known answers: (edges, n, source, expected_distances) -------------
# Distances hand-computed with the weight formula above; +inf for unreachable.
INF = float("inf")
SSSP_CASES = {
    # tri_chain_5: w(0,1)=12, w(0,2)=3, w(1,3)=12, w(2,3)=4, w(3,4)=13
    # d0=0, d1=12, d2=3, d3=min(12+12, 3+4)=7, d4=7+13=20
    "tri_chain_5": (
        [(0, 1), (0, 2), (1, 3), (2, 3), (3, 4)], 5, 0, [0.0, 12.0, 3.0, 7.0, 20.0],
    ),
    # path_5: w(0,1)=12, w(1,2)=8, w(2,3)=4, w(3,4)=13 -> [0,12,20,24,37]
    "path_5": (
        [(0, 1), (1, 2), (2, 3), (3, 4)], 5, 0, [0.0, 12.0, 20.0, 24.0, 37.0],
    ),
    # unreachable second component. w(0,1)=1+((17+7)%13)=1+11=12
    "two_edges": ([(0, 1), (2, 3)], 4, 0, [0.0, 12.0, INF, INF]),
}

# ---- PageRank known answers (symmetric graphs -> closed-form) ---------------
# (edges, n, expected_ranks, abs_eps)
PR_CASES = {
    # directed 3-cycle: stationary distribution is uniform 1/3 each
    "cycle_3": ([(0, 1), (1, 2), (2, 0)], 3, [1 / 3, 1 / 3, 1 / 3], 1e-4),
    # directed 2-cycle: uniform 1/2 each
    "cycle_2": ([(0, 1), (1, 0)], 2, [1 / 2, 1 / 2], 1e-4),
    # directed 4-cycle: uniform 1/4 each
    "cycle_4": ([(0, 1), (1, 2), (2, 3), (3, 0)], 4, [0.25, 0.25, 0.25, 0.25], 1e-4),
}

# ---- LP known answer with a deliberate tie --------------------------------
# Node 2 (unseeded) points to node 0 (seed label 5) and node 1 (seed label 3).
# Neighbour labels {5,3}: 1 each -> tie -> smallest id -> 3.
# Expected full vector: [5, 3, 3]. seeds = {0:5, 1:3}.
LP_TIE_CASE = {
    "edges": [(2, 0), (2, 1)],
    "n": 3,
    "seeds": {0: 5, 1: 3},
    "expected_labels": [5, 3, 3],
}
