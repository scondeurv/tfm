"""Correctness validation against an INDEPENDENT oracle (networkx) + hand-computed
known answers + property-based testing.

This complements test_cross_backend_correctness.py / test_determinism_proof.py
(which prove backends AGREE with the standalone reference) by proving the
reference itself is CORRECT — closing the shared-bug gap. Strategy:

- Known-answer: standalone binary AND the oracle must both match hand-computed
  truth (independent of each other and of networkx).
- Property-based (hypothesis): on random digraphs, the standalone binary must
  match the oracle (BFS exact, SSSP within fp tol, PageRank within eps); LP must
  satisfy the semi-supervised fixed-point invariants.
- Metamorphic: node-id permutation invariance.

Run under tests/.venv-test (needs networkx, hypothesis, numpy, scipy):
    tests/.venv-test/bin/python -m unittest tests.test_oracle_correctness -v
"""
from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

from tests import known_answers as ka
from tests.determinism_fixtures import edges_to_tsv_unweighted, edges_to_tsv_weighted
from tests.test_cross_backend_correctness import _BINARIES, _require_binaries, _run_binary

# Optional deps (networkx/hypothesis/numpy/scipy) live in tests/.venv-test. When
# absent (e.g. the suite is discovered under the orchestration venv), skip this
# module cleanly instead of erroring. Stubs below let class bodies that use
# @given/@st define without raising; the skipUnless guards prevent execution.
try:
    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st
    from tests import oracle

    HAVE_DEPS = True
    DEPS_REASON = ""
except Exception as _e:  # pragma: no cover - exercised only without test deps
    HAVE_DEPS = False
    DEPS_REASON = f"correctness deps missing ({_e}); run under tests/.venv-test"
    oracle = None  # type: ignore

    def given(*_a, **_k):
        return lambda f: f

    def settings(*_a, **_k):
        return lambda f: f

    class HealthCheck:  # noqa: N801 - mimic enum holder
        too_slow = None

    class _St:
        def composite(self, _f):  # returns a strategy-factory stub
            return lambda *a, **k: None

        def __getattr__(self, _name):
            return lambda *a, **k: None

    st = _St()  # type: ignore

U32_MAX = 2**32 - 1


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def _norm_distances(raw: list) -> list[float]:
    """Standalone serializes unreachable f32::INFINITY as JSON null."""
    return [math.inf if x is None else float(x) for x in raw]


def _write(td: str, name: str, text: str) -> Path:
    p = Path(td) / name
    p.write_text(text)
    return p


def _run_bfs(edges, n, source=0, max_levels=10_000) -> list[int]:
    with tempfile.TemporaryDirectory() as td:
        g = _write(td, "g.tsv", edges_to_tsv_unweighted(edges))
        out = _run_binary(_BINARIES["bfs_standalone"], [str(g), str(n), str(source), str(max_levels)])
    return [int(x) for x in out["levels"]]


def _run_sssp(edges, n, source=0, max_iter=10_000) -> list[float]:
    with tempfile.TemporaryDirectory() as td:
        g = _write(td, "g.tsv", edges_to_tsv_weighted(edges))
        out = _run_binary(_BINARIES["sssp_standalone"], [str(g), str(n), str(source), str(max_iter)])
    return _norm_distances(out["distances"])


def _run_pagerank(edges, n, max_iter=1000) -> list[float]:
    with tempfile.TemporaryDirectory() as td:
        g = _write(td, "g.tsv", edges_to_tsv_unweighted(edges))
        out = _run_binary(_BINARIES["pagerank_standalone"], [str(g), str(n), str(max_iter)])
    return [float(x) for x in out["rank"]]


def _run_lp(edges, n, seeds: dict[int, int], max_iter=200) -> list[int]:
    """Build a graph file with seeds embedded on the source column. Seed nodes
    without an out-edge get a self-loop line so their seed is recorded."""
    lines: list[str] = []
    srcs_with_edges = {u for u, _ in edges}
    for s in seeds:
        if s not in srcs_with_edges:
            lines.append(f"{s}\t{s}\t{seeds[s]}\n")  # self-loop carries the seed
    for (u, v) in edges:
        if u in seeds:
            lines.append(f"{u}\t{v}\t{seeds[u]}\n")
        else:
            lines.append(f"{u}\t{v}\n")
    with tempfile.TemporaryDirectory() as td:
        g = _write(td, "g.tsv", "".join(lines))
        out = _run_binary(_BINARIES["lp_standalone"], [str(g), str(n), str(max_iter)])
    return [int(x) for x in out["labels"]]


def _assert_dist_close(a: list[float], b: list[float], label: str, tol=1e-4):
    assert len(a) == len(b), f"{label}: length {len(a)} vs {len(b)}"
    for i, (x, y) in enumerate(zip(a, b)):
        if math.isinf(x) and math.isinf(y):
            continue
        assert abs(x - y) <= tol, f"{label}: dist[{i}] {x} vs {y} (tol {tol})"


def _assert_rank_close(a: list[float], b: list[float], label: str, eps=1e-3):
    assert len(a) == len(b), f"{label}: length {len(a)} vs {len(b)}"
    md = max((abs(x - y) for x, y in zip(a, b)), default=0.0)
    assert md <= eps, f"{label}: max|Δrank|={md:.3e} > eps {eps}"


# ----------------------------------------------------------------------------
# Known-answer tests: standalone AND oracle vs hand-computed truth.
# ----------------------------------------------------------------------------
@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class KnownAnswerBFS(unittest.TestCase):
    def test_bfs(self):
        _require_binaries("bfs_standalone")
        for name, (edges, n, src, expected) in ka.BFS_CASES.items():
            with self.subTest(case=name):
                self.assertEqual(oracle.bfs_levels(edges, n, src), expected, f"oracle/{name}")
                self.assertEqual(_run_bfs(edges, n, src), expected, f"standalone/{name}")


@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class KnownAnswerSSSP(unittest.TestCase):
    def test_sssp(self):
        _require_binaries("sssp_standalone")
        for name, (edges, n, src, expected) in ka.SSSP_CASES.items():
            with self.subTest(case=name):
                orac = oracle.sssp_distances(oracle.weighted_edges_from_unweighted(edges), n, src)
                _assert_dist_close(orac, expected, f"oracle/{name}", tol=1e-9)
                _assert_dist_close(_run_sssp(edges, n, src), expected, f"standalone/{name}", tol=1e-3)


@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class KnownAnswerPageRank(unittest.TestCase):
    def test_pagerank(self):
        _require_binaries("pagerank_standalone")
        for name, (edges, n, expected, eps) in ka.PR_CASES.items():
            with self.subTest(case=name):
                _assert_rank_close(oracle.pagerank(edges, n), expected, f"oracle/{name}", eps)
                _assert_rank_close(_run_pagerank(edges, n), expected, f"standalone/{name}", eps)


@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class KnownAnswerLPTie(unittest.TestCase):
    def test_lp_tie_breaks_to_smallest(self):
        _require_binaries("lp_standalone")
        c = ka.LP_TIE_CASE
        labels = _run_lp(c["edges"], c["n"], c["seeds"])
        self.assertEqual(labels, c["expected_labels"], "LP tie must break to smallest label id")
        # The same vector must satisfy the LP fixed-point invariants.
        ok, diag = oracle.lp_invariants(c["edges"], c["n"], labels, c["seeds"])
        self.assertTrue(ok, diag)


# ----------------------------------------------------------------------------
# Property-based: standalone vs oracle on random digraphs.
# ----------------------------------------------------------------------------
@st.composite
def digraphs(draw, min_n=2, max_n=16, allow_self_loops=False):
    n = draw(st.integers(min_n, max_n))
    pairs = [(u, v) for u in range(n) for v in range(n) if allow_self_loops or u != v]
    edges = draw(st.lists(st.sampled_from(pairs), min_size=0, max_size=min(len(pairs), 4 * n)))
    return edges, n


PROP = settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class OracleProperty(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _require_binaries("bfs_standalone", "sssp_standalone", "pagerank_standalone", "lp_standalone")

    @PROP
    @given(digraphs())
    def test_bfs_matches_oracle(self, gn):
        edges, n = gn
        self.assertEqual(_run_bfs(edges, n, 0), oracle.bfs_levels(edges, n, 0))

    @PROP
    @given(digraphs())
    def test_sssp_matches_oracle(self, gn):
        edges, n = gn
        orac = oracle.sssp_distances(oracle.weighted_edges_from_unweighted(edges), n, 0)
        _assert_dist_close(_run_sssp(edges, n, 0), orac, "SSSP-prop", tol=1e-3)

    @PROP
    @given(digraphs())
    def test_pagerank_matches_oracle(self, gn):
        edges, n = gn
        _assert_rank_close(_run_pagerank(edges, n), oracle.pagerank(edges, n), "PR-prop", eps=1e-3)

    @PROP
    @given(digraphs(), st.data())
    def test_lp_invariants_hold(self, gn, data):
        edges, n = gn
        # seed ~20% of nodes with arbitrary labels in a stable id range
        k = max(1, n // 5)
        seed_nodes = data.draw(st.lists(st.integers(0, n - 1), min_size=1, max_size=k, unique=True))
        seeds = {s: data.draw(st.integers(0, 9)) * 100 + s for s in seed_nodes}
        # include the self-loops we add for seed nodes in the edge set we check
        eff_edges = list(edges) + [(s, s) for s in seeds if s not in {u for u, _ in edges}]
        labels = _run_lp(edges, n, seeds)
        ok, diag = oracle.lp_invariants(eff_edges, n, labels, seeds)
        self.assertTrue(ok, diag)


# ----------------------------------------------------------------------------
# Metamorphic: permuting node ids permutes the result vector accordingly.
# ----------------------------------------------------------------------------
@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class Metamorphic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _require_binaries("bfs_standalone")

    @settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(digraphs(min_n=3, max_n=12), st.randoms(use_true_random=False))
    def test_bfs_permutation_invariance(self, gn, rng):
        edges, n = gn
        perm = list(range(n))
        rng.shuffle(perm)
        # keep source fixed by mapping: relabel so old 0 -> new perm[0], compare
        relabeled = [(perm[u], perm[v]) for (u, v) in edges]
        base = _run_bfs(edges, n, 0)
        permuted = _run_bfs(relabeled, n, perm[0])
        # permuted[perm[i]] must equal base[i]
        for i in range(n):
            self.assertEqual(permuted[perm[i]], base[i], f"perm invariance node {i}")


# ----------------------------------------------------------------------------
# Oracle-as-root over the shared determinism fixtures: proves the standalone
# reference is correct on exactly the topologies the cross-backend/determinism
# suites use to anchor every other paradigm (so a reference bug can't hide).
# ----------------------------------------------------------------------------
@unittest.skipUnless(HAVE_DEPS, DEPS_REASON)
class FixtureOracle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _require_binaries("bfs_standalone", "sssp_standalone", "pagerank_standalone")

    def test_all_fixtures_vs_oracle(self):
        from tests.determinism_fixtures import FIXTURES

        for name, gen in FIXTURES.items():
            edges, n = gen()
            with self.subTest(fixture=name):
                self.assertEqual(_run_bfs(edges, n, 0), oracle.bfs_levels(edges, n, 0), f"BFS/{name}")
                orac = oracle.sssp_distances(oracle.weighted_edges_from_unweighted(edges), n, 0)
                _assert_dist_close(_run_sssp(edges, n, 0), orac, f"SSSP/{name}", tol=1e-2)
                _assert_rank_close(_run_pagerank(edges, n), oracle.pagerank(edges, n), f"PR/{name}", eps=1e-3)


if __name__ == "__main__":
    unittest.main()
