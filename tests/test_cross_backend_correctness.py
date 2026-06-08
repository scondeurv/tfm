"""Deterministic cross-backend correctness tests.

For each COST algorithm (BFS, SSSP, PageRank), build a fixed-graph fixture and
verify that the standalone and Rayon binaries agree on the algorithm-level
output. LP is excluded for now: the Rayon LP binary does not emit the final
label vector in JSON, so a hash-based comparison is not yet feasible without
extending the Rust crate. BFS/SSSP/PageRank do emit the canonical aggregate
fields (`max_level` / `max_distance` / `max_rank` + counts), which are
sufficient for a deterministic equality check.

Tests skip automatically when a binary is missing (e.g. MPI builds that
require OpenMPI 4.x on CloudLab).
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_BINARIES = {
    "lp_standalone": ROOT / "labelpropagation/lpst/target/release/label-propagation",
    "lp_rayon": ROOT / "labelpropagation/lp-rayon/target/release/lp-rayon",
    "lp_mpi": ROOT / "labelpropagation/lp-mpi/target/release/lp-mpi",
    "bfs_standalone": ROOT / "bfs/bfs-standalone/target/release/bfs-standalone",
    "bfs_rayon": ROOT / "bfs/bfs-rayon/target/release/bfs-rayon",
    "bfs_mpi": ROOT / "bfs/bfs-mpi/target/release/bfs-mpi",
    "sssp_standalone": ROOT / "sssp/sssp-standalone/target/release/sssp-standalone",
    "sssp_rayon": ROOT / "sssp/sssp-rayon/target/release/sssp-rayon",
    "sssp_mpi": ROOT / "sssp/sssp-mpi/target/release/sssp-mpi",
    "pagerank_standalone": ROOT / "pagerank/pagerank-standalone/target/release/pagerank-standalone",
    "pagerank_rayon": ROOT / "pagerank/pagerank-rayon/target/release/pagerank-rayon",
    "pagerank_mpi": ROOT / "pagerank/pagerank-mpi/target/release/pagerank-mpi",
}


def _require_mpirun() -> None:
    if shutil.which("mpirun") is None:
        raise unittest.SkipTest("mpirun not on PATH (install OpenMPI 4.x to enable MPI tests)")


def _run_mpi_binary(binary: Path, ranks: int, args: list[str], timeout: int = 120) -> dict:
    cmd = ["mpirun", "--oversubscribe", "-np", str(ranks), str(binary), *args]
    return _exec(cmd, timeout=timeout)


def _exec(cmd: list[str], timeout: int) -> dict:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(
            f"{cmd[0]} failed (rc={result.returncode}):\n"
            f"CMD: {' '.join(cmd)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    last_json = None
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                last_json = json.loads(line)
            except json.JSONDecodeError:
                pass
    if last_json is None:
        raise RuntimeError(f"{cmd[0]} produced no JSON record")
    return last_json


def _require_binaries(*names: str) -> None:
    missing = [n for n in names if not _BINARIES[n].exists()]
    if missing:
        raise unittest.SkipTest(f"binaries not built locally: {missing}")


def _run_binary(binary: Path, args: list[str], timeout: int = 60) -> dict:
    return _exec([str(binary), *args], timeout=timeout)


# ----------------------------------------------------------------------------
# BFS: directed unweighted graph, source=0. Expect deterministic BFS levels.
# Topology (5 nodes):
#   0 → 1, 0 → 2, 1 → 3, 2 → 3, 3 → 4
# Levels from 0: [0, 1, 1, 2, 3]; max_level=3, visited_nodes=5.
# ----------------------------------------------------------------------------
_BFS_FIXTURE = "0\t1\n0\t2\n1\t3\n2\t3\n3\t4\n"


class BfsCrossBackendTests(unittest.TestCase):
    def test_bfs_standalone_matches_rayon(self) -> None:
        _require_binaries("bfs_standalone", "bfs_rayon")
        with tempfile.TemporaryDirectory() as td:
            graph = Path(td) / "bfs.tsv"
            graph.write_text(_BFS_FIXTURE)
            standalone = _run_binary(_BINARIES["bfs_standalone"], [str(graph), "5", "0", "100"])
            for threads in (1, 2, 4):
                with self.subTest(threads=threads):
                    rayon = _run_binary(_BINARIES["bfs_rayon"], [str(graph), "5", "0", "100", str(threads)])
                    self.assertEqual(standalone["max_level"], rayon["max_level"])
                    self.assertEqual(standalone["visited_nodes"], rayon["visited_nodes"])


# ----------------------------------------------------------------------------
# SSSP: weighted directed graph. f32 distances → bit-exact equality expected.
# ----------------------------------------------------------------------------
_SSSP_FIXTURE = (
    "0\t1\t1.0\n"
    "0\t2\t4.0\n"
    "1\t2\t2.0\n"
    "1\t3\t6.0\n"
    "2\t3\t1.5\n"
    "2\t4\t10.0\n"
    "3\t4\t2.0\n"
)


class SsspCrossBackendTests(unittest.TestCase):
    def test_sssp_standalone_matches_rayon(self) -> None:
        _require_binaries("sssp_standalone", "sssp_rayon")
        with tempfile.TemporaryDirectory() as td:
            graph = Path(td) / "sssp.tsv"
            graph.write_text(_SSSP_FIXTURE)
            standalone = _run_binary(_BINARIES["sssp_standalone"], [str(graph), "5", "0", "100"])
            for threads in (1, 2, 4):
                with self.subTest(threads=threads):
                    rayon = _run_binary(_BINARIES["sssp_rayon"], [str(graph), "5", "0", "100", str(threads)])
                    # f32 distances should be bit-identical across backends.
                    self.assertEqual(standalone["max_distance"], rayon["max_distance"])
                    self.assertEqual(standalone["reachable_nodes"], rayon["reachable_nodes"])


# ----------------------------------------------------------------------------
# PageRank: damping=0.85, tolerance=1e-6. Power iteration converges to a
# stationary distribution that must match across backends to within float
# tolerance.
# ----------------------------------------------------------------------------
_PAGERANK_FIXTURE = (
    "0\t1\n"
    "1\t2\n"
    "2\t0\n"
    "0\t2\n"
    "3\t0\n"
    "4\t3\n"
)


class PageRankCrossBackendTests(unittest.TestCase):
    def test_pagerank_standalone_matches_rayon(self) -> None:
        _require_binaries("pagerank_standalone", "pagerank_rayon")
        with tempfile.TemporaryDirectory() as td:
            graph = Path(td) / "pr.tsv"
            graph.write_text(_PAGERANK_FIXTURE)
            standalone = _run_binary(_BINARIES["pagerank_standalone"], [str(graph), "5", "200"])
            self.assertAlmostEqual(standalone["sum_rank"], 1.0, places=3)
            for threads in (1, 2, 4):
                with self.subTest(threads=threads):
                    rayon = _run_binary(_BINARIES["pagerank_rayon"], [str(graph), "5", "200", str(threads)])
                    self.assertEqual(standalone["iterations"], rayon["iterations"])
                    self.assertAlmostEqual(
                        standalone["max_rank"], rayon["max_rank"], places=5,
                        msg=f"max_rank mismatch standalone vs rayon-{threads}",
                    )
                    self.assertAlmostEqual(rayon["sum_rank"], 1.0, places=3)


# ----------------------------------------------------------------------------
# MPI cross-backend tests. Skipped automatically when:
#   - the MPI binary is not built (e.g. local OpenMPI 5.x incompatible with
#     rsmpi 0.8); or
#   - mpirun is not on PATH.
# Ranks {1, 2, 4} cover degenerate single-rank, even split, uneven split via
# `owned_range`'s remainder handling.
# ----------------------------------------------------------------------------


class BfsMpiCrossBackendTests(unittest.TestCase):
    def test_bfs_standalone_matches_mpi(self) -> None:
        _require_binaries("bfs_standalone", "bfs_mpi")
        _require_mpirun()
        with tempfile.TemporaryDirectory() as td:
            graph = Path(td) / "bfs.tsv"
            graph.write_text(_BFS_FIXTURE)
            standalone = _run_binary(_BINARIES["bfs_standalone"], [str(graph), "5", "0", "100"])
            for ranks in (1, 2, 4):
                with self.subTest(ranks=ranks):
                    mpi = _run_mpi_binary(
                        _BINARIES["bfs_mpi"], ranks, [str(graph), "5", "0", "100"]
                    )
                    self.assertEqual(standalone["max_level"], mpi["max_level"])
                    self.assertEqual(standalone["visited_nodes"], mpi["visited_nodes"])


class SsspMpiCrossBackendTests(unittest.TestCase):
    def test_sssp_standalone_matches_mpi(self) -> None:
        _require_binaries("sssp_standalone", "sssp_mpi")
        _require_mpirun()
        with tempfile.TemporaryDirectory() as td:
            graph = Path(td) / "sssp.tsv"
            graph.write_text(_SSSP_FIXTURE)
            standalone = _run_binary(_BINARIES["sssp_standalone"], [str(graph), "5", "0", "100"])
            for ranks in (1, 2, 4):
                with self.subTest(ranks=ranks):
                    mpi = _run_mpi_binary(
                        _BINARIES["sssp_mpi"], ranks, [str(graph), "5", "0", "100"]
                    )
                    self.assertEqual(standalone["max_distance"], mpi["max_distance"])
                    self.assertEqual(standalone["reachable_nodes"], mpi["reachable_nodes"])


class PageRankMpiCrossBackendTests(unittest.TestCase):
    def test_pagerank_standalone_matches_mpi(self) -> None:
        _require_binaries("pagerank_standalone", "pagerank_mpi")
        _require_mpirun()
        with tempfile.TemporaryDirectory() as td:
            graph = Path(td) / "pr.tsv"
            graph.write_text(_PAGERANK_FIXTURE)
            standalone = _run_binary(_BINARIES["pagerank_standalone"], [str(graph), "5", "200"])
            for ranks in (1, 2, 4):
                with self.subTest(ranks=ranks):
                    mpi = _run_mpi_binary(
                        _BINARIES["pagerank_mpi"], ranks, [str(graph), "5", "200"]
                    )
                    self.assertEqual(standalone["iterations"], mpi["iterations"])
                    self.assertAlmostEqual(
                        standalone["max_rank"], mpi["max_rank"], places=5,
                        msg=f"max_rank mismatch standalone vs mpi-{ranks}",
                    )
                    self.assertAlmostEqual(mpi["sum_rank"], 1.0, places=3)


if __name__ == "__main__":
    unittest.main()
