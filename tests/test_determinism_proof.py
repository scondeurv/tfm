"""Cross-backend determinism proof.

For each algorithm (LP, BFS, SSSP, PageRank) and each fixture in
`determinism_fixtures.FIXTURES`, run the standalone (reference) and the
parallel backends (rayon at multiple thread counts, MPI at multiple rank
counts when available) and assert FULL-VECTOR agreement using algo-specific
comparators.

Backends covered locally:
- standalone (ground truth)
- rayon (1/2/4 threads)
- mpi  (1/2/4 ranks, skipped if mpirun missing or *_mpi binary not built)

Cluster backends (Burst, Spark) are validated by
`campaigns/validate_implementations_cluster.sh`.
"""
from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.determinism_compare import (
    assert_bfs_levels_equal,
    assert_lp_partition_equivalent,
    assert_pagerank_ranks_close,
    assert_sssp_distances_equal,
)
from tests.determinism_fixtures import (
    FIXTURES,
    edges_to_tsv_unweighted,
    edges_to_tsv_weighted,
)
from tests.test_cross_backend_correctness import (
    _BINARIES,
    _exec,
    _require_binaries,
    _require_mpirun,
    _run_binary,
    _run_mpi_binary,
)

RAYON_THREADS = (1, 2, 4)
MPI_RANKS = (1, 2, 4)
BFS_MAX_LEVELS = 10_000
SSSP_MAX_ITERS = 10_000
PR_MAX_ITER = 500
LP_MAX_ITER = 200


def _write_unweighted(td: Path, name: str, edges) -> Path:
    p = td / f"{name}.tsv"
    p.write_text(edges_to_tsv_unweighted(edges))
    return p


def _write_weighted(td: Path, name: str, edges) -> Path:
    p = td / f"{name}.tsv"
    p.write_text(edges_to_tsv_weighted(edges))
    return p


# ============================================================================
# BFS — full levels[] vector compare. Source = vertex 0 by convention.
# ============================================================================


class BfsDeterminism(unittest.TestCase):
    def _run_fixture(self, fixture_name: str) -> None:
        edges, n = FIXTURES[fixture_name]()
        with tempfile.TemporaryDirectory() as td:
            p = _write_unweighted(Path(td), fixture_name, edges)
            ref = _run_binary(
                _BINARIES["bfs_standalone"], [str(p), str(n), "0", str(BFS_MAX_LEVELS)]
            )
            for t in RAYON_THREADS:
                with self.subTest(backend=f"rayon-{t}", fixture=fixture_name):
                    cmp = _run_binary(
                        _BINARIES["bfs_rayon"], [str(p), str(n), "0", str(BFS_MAX_LEVELS), str(t)]
                    )
                    assert_bfs_levels_equal(ref, cmp, f"BFS rayon-{t}/{fixture_name}")
            if _BINARIES["bfs_mpi"].exists() and shutil.which("mpirun"):
                for r in MPI_RANKS:
                    with self.subTest(backend=f"mpi-{r}", fixture=fixture_name):
                        cmp = _run_mpi_binary(
                            _BINARIES["bfs_mpi"], r, [str(p), str(n), "0", str(BFS_MAX_LEVELS)]
                        )
                        assert_bfs_levels_equal(cmp, ref, f"BFS mpi-{r}/{fixture_name}")


def _make_bfs_test(name: str):
    def test(self):
        _require_binaries("bfs_standalone", "bfs_rayon")
        self._run_fixture(name)

    test.__name__ = f"test_bfs_{name}"
    return test


for _name in FIXTURES:
    setattr(BfsDeterminism, f"test_bfs_{_name}", _make_bfs_test(_name))


# ============================================================================
# SSSP — bit-exact f32 distances[] compare.
# ============================================================================


class SsspDeterminism(unittest.TestCase):
    def _run_fixture(self, fixture_name: str) -> None:
        edges, n = FIXTURES[fixture_name]()
        with tempfile.TemporaryDirectory() as td:
            p = _write_weighted(Path(td), fixture_name, edges)
            ref = _run_binary(
                _BINARIES["sssp_standalone"], [str(p), str(n), "0", str(SSSP_MAX_ITERS)]
            )
            for t in RAYON_THREADS:
                with self.subTest(backend=f"rayon-{t}", fixture=fixture_name):
                    cmp = _run_binary(
                        _BINARIES["sssp_rayon"],
                        [str(p), str(n), "0", str(SSSP_MAX_ITERS), str(t)],
                    )
                    assert_sssp_distances_equal(ref, cmp, f"SSSP rayon-{t}/{fixture_name}")
            if _BINARIES["sssp_mpi"].exists() and shutil.which("mpirun"):
                for r in MPI_RANKS:
                    with self.subTest(backend=f"mpi-{r}", fixture=fixture_name):
                        cmp = _run_mpi_binary(
                            _BINARIES["sssp_mpi"], r, [str(p), str(n), "0", str(SSSP_MAX_ITERS)]
                        )
                        assert_sssp_distances_equal(cmp, ref, f"SSSP mpi-{r}/{fixture_name}")


def _make_sssp_test(name: str):
    def test(self):
        _require_binaries("sssp_standalone", "sssp_rayon")
        self._run_fixture(name)

    test.__name__ = f"test_sssp_{name}"
    return test


for _name in FIXTURES:
    setattr(SsspDeterminism, f"test_sssp_{_name}", _make_sssp_test(_name))


# ============================================================================
# PageRank — element-wise rank[] compare within ε=1e-5.
# ============================================================================


class PageRankDeterminism(unittest.TestCase):
    def _run_fixture(self, fixture_name: str) -> None:
        edges, n = FIXTURES[fixture_name]()
        with tempfile.TemporaryDirectory() as td:
            p = _write_unweighted(Path(td), fixture_name, edges)
            ref = _run_binary(
                _BINARIES["pagerank_standalone"], [str(p), str(n), str(PR_MAX_ITER)]
            )
            for t in RAYON_THREADS:
                with self.subTest(backend=f"rayon-{t}", fixture=fixture_name):
                    cmp = _run_binary(
                        _BINARIES["pagerank_rayon"],
                        [str(p), str(n), str(PR_MAX_ITER), str(t)],
                    )
                    assert_pagerank_ranks_close(
                        ref, cmp, f"PR rayon-{t}/{fixture_name}", eps=1e-5
                    )
            if _BINARIES["pagerank_mpi"].exists() and shutil.which("mpirun"):
                for r in MPI_RANKS:
                    with self.subTest(backend=f"mpi-{r}", fixture=fixture_name):
                        cmp = _run_mpi_binary(
                            _BINARIES["pagerank_mpi"], r,
                            [str(p), str(n), str(PR_MAX_ITER)],
                        )
                        assert_pagerank_ranks_close(
                            ref, cmp, f"PR mpi-{r}/{fixture_name}", eps=1e-5
                        )


def _make_pr_test(name: str):
    def test(self):
        _require_binaries("pagerank_standalone", "pagerank_rayon")
        self._run_fixture(name)

    test.__name__ = f"test_pagerank_{name}"
    return test


for _name in FIXTURES:
    setattr(PageRankDeterminism, f"test_pagerank_{_name}", _make_pr_test(_name))


# ============================================================================
# LP — partition equivalence over labels[]. Label IDs arbitrary; any consistent
# relabeling is acceptable.
# ============================================================================


class LpDeterminism(unittest.TestCase):
    def _run_fixture(self, fixture_name: str) -> None:
        edges, n = FIXTURES[fixture_name]()
        with tempfile.TemporaryDirectory() as td:
            p = _write_unweighted(Path(td), fixture_name, edges)
            ref = _run_binary(
                _BINARIES["lp_standalone"], [str(p), str(n), str(LP_MAX_ITER)]
            )
            for t in RAYON_THREADS:
                with self.subTest(backend=f"rayon-{t}", fixture=fixture_name):
                    cmp = _run_binary(
                        _BINARIES["lp_rayon"], [str(p), str(n), str(LP_MAX_ITER), str(t)]
                    )
                    assert_lp_partition_equivalent(
                        ref, cmp, f"LP rayon-{t}/{fixture_name}"
                    )
            if _BINARIES["lp_mpi"].exists() and shutil.which("mpirun"):
                for r in MPI_RANKS:
                    with self.subTest(backend=f"mpi-{r}", fixture=fixture_name):
                        cmp = _run_mpi_binary(
                            _BINARIES["lp_mpi"], r, [str(p), str(n), str(LP_MAX_ITER)]
                        )
                        assert_lp_partition_equivalent(
                            ref, cmp, f"LP mpi-{r}/{fixture_name}"
                        )


def _make_lp_test(name: str):
    def test(self):
        _require_binaries("lp_standalone", "lp_rayon")
        self._run_fixture(name)

    test.__name__ = f"test_lp_{name}"
    return test


for _name in FIXTURES:
    setattr(LpDeterminism, f"test_lp_{_name}", _make_lp_test(_name))


if __name__ == "__main__":
    unittest.main()
