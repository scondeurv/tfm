"""Unit tests for the unified COST sweep orchestrator + report generators.

These tests verify pure-Python parts of the orchestration layer without
spawning SSH or matplotlib. Targets:

  campaigns/cost_backends.py:
      - expand_cost_cells() enumeration shape
      - _parse_benchmark_stdout() JSON extraction
      - _wrap_result() shape

  campaigns/report_generators.py:
      - render_cost_report() against synthetic runs_*.json fixtures
      - render_cross_backend_table()
      - render_campaign_summary() (README.md + summary.md)

These complement test_burst_compute_only_proxy.py.
"""
from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CAMPAIGNS = ROOT / "campaigns"
if str(CAMPAIGNS) not in sys.path:
    sys.path.insert(0, str(CAMPAIGNS))

import cost_backends  # noqa: E402
import report_generators  # noqa: E402


class ExpandCostCellsTests(unittest.TestCase):
    def test_standalone_only_yields_one_cell_per_node_rep(self) -> None:
        cells = cost_backends.expand_cost_cells(
            backends=["standalone"],
            nodes_list=[10_000, 100_000],
            reps=3,
            rayon_threads=[],
            mpi_ranks=[],
        )
        self.assertEqual(len(cells), 2 * 3)
        self.assertTrue(all(c["backend"] == "standalone" for c in cells))

    def test_full_mix_expands_combinatorially(self) -> None:
        cells = cost_backends.expand_cost_cells(
            backends=["standalone", "rayon", "mpi"],
            nodes_list=[1000],
            reps=2,
            rayon_threads=[1, 4],
            mpi_ranks=[4, 8],
        )
        # standalone: 1 n × 2 reps = 2
        # rayon: 1 n × 2 threads × 2 reps = 4
        # mpi: 1 n × 2 ranks × 2 reps = 4
        self.assertEqual(len(cells), 2 + 4 + 4)
        rayon_cells = [c for c in cells if c["backend"] == "rayon"]
        self.assertEqual({c["threads"] for c in rayon_cells}, {1, 4})
        mpi_cells = [c for c in cells if c["backend"] == "mpi"]
        self.assertEqual({c["ranks"] for c in mpi_cells}, {4, 8})

    def test_empty_backends_returns_empty(self) -> None:
        self.assertEqual(
            cost_backends.expand_cost_cells(
                backends=[], nodes_list=[1000], reps=1,
                rayon_threads=[1], mpi_ranks=[1],
            ),
            [],
        )


class ParseBenchmarkStdoutTests(unittest.TestCase):
    def test_extracts_last_top_level_json(self) -> None:
        stdout = textwrap.dedent("""
            Loading graph...
            {"intermediate": true}
            Iter 1 done
            {"execution_time_ms": 1234, "iterations": 17}
        """).strip()
        result = cost_backends._parse_benchmark_stdout(stdout)
        self.assertEqual(result, {"execution_time_ms": 1234, "iterations": 17})

    def test_no_json_returns_none(self) -> None:
        self.assertIsNone(cost_backends._parse_benchmark_stdout("just progress\n"))

    def test_malformed_json_skipped(self) -> None:
        stdout = "{not json}\n{\"good\": 1}\n"
        self.assertEqual(cost_backends._parse_benchmark_stdout(stdout), {"good": 1})


class WrapResultTests(unittest.TestCase):
    def test_passed_when_rc_zero_and_json_parseable(self) -> None:
        result = cost_backends._wrap_result(
            "standalone", 0, '{"execution_time_ms": 100}\n', "",
        )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["backend"], "standalone")
        self.assertEqual(result["raw"], {"execution_time_ms": 100})

    def test_failed_when_rc_nonzero(self) -> None:
        result = cost_backends._wrap_result(
            "rayon", 1, "", "boom", extras={"threads": 4},
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["threads"], 4)
        self.assertEqual(result["error"], "boom")


def _synthetic_cost_runs() -> tuple[list[dict], list[dict], list[dict]]:
    """LP: standalone slow, rayon-4 fast at large n, mpi-4 medium."""
    standalone = [
        {"algorithm": "lp", "nodes": 1000, "rep": 0, "status": "passed",
         "result": {"raw": {"execution_time_ms": 100}}},
        {"algorithm": "lp", "nodes": 1_000_000, "rep": 0, "status": "passed",
         "result": {"raw": {"execution_time_ms": 100_000}}},
    ]
    rayon = [
        {"algorithm": "lp", "nodes": 1000, "rep": 0, "threads": 4, "status": "passed",
         "result": {"raw": {"execution_time_ms": 200}}},
        {"algorithm": "lp", "nodes": 1_000_000, "rep": 0, "threads": 4, "status": "passed",
         "result": {"raw": {"execution_time_ms": 30_000}}},
    ]
    mpi = [
        {"algorithm": "lp", "nodes": 1000, "rep": 0, "ranks": 4, "status": "passed",
         "result": {"raw": {"execution_time_ms": 500}}},
        {"algorithm": "lp", "nodes": 1_000_000, "rep": 0, "ranks": 4, "status": "passed",
         "result": {"raw": {"execution_time_ms": 50_000}}},
    ]
    return standalone, rayon, mpi


class RenderCostReportTests(unittest.TestCase):
    def test_writes_table_and_crossover(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            campaign_root = Path(td)
            cost_dir = campaign_root / "cost_sweep"
            cost_dir.mkdir(parents=True)
            standalone, rayon, mpi = _synthetic_cost_runs()
            (cost_dir / "runs_standalone.json").write_text(json.dumps(standalone))
            (cost_dir / "runs_rayon.json").write_text(json.dumps(rayon))
            (cost_dir / "runs_mpi.json").write_text(json.dumps(mpi))

            out = campaign_root / "report" / "lp"
            report_generators.render_cost_report(campaign_root, "lp", out)

            table = (out / "cost_table.md").read_text()
            self.assertIn("standalone median ms", table)
            self.assertIn("rayon-4 median ms", table)
            self.assertIn("mpi-4 median ms", table)
            self.assertIn("1,000,000", table)

            crossover = json.loads((out / "cost_crossover.json").read_text())
            # rayon-4 beats standalone at the small n in this fixture (200 > 100 actually false)
            # but at large n rayon-4 is 30k vs 100k → crossover at 1000? No: 200>100 at 1000.
            # So crossover should be n=1_000_000.
            self.assertEqual(crossover.get("rayon-4"), 1_000_000)
            self.assertEqual(crossover.get("mpi-4"), 1_000_000)

    def test_noop_when_cost_sweep_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            campaign_root = Path(td)
            out = campaign_root / "report" / "lp"
            report_generators.render_cost_report(campaign_root, "lp", out)
            self.assertFalse((out / "cost_table.md").exists())


class CrossBackendTableTests(unittest.TestCase):
    def test_picks_best_per_n(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            campaign_root = Path(td)
            cost_dir = campaign_root / "cost_sweep"
            cost_dir.mkdir(parents=True)
            standalone, rayon, mpi = _synthetic_cost_runs()
            (cost_dir / "runs_standalone.json").write_text(json.dumps(standalone))
            (cost_dir / "runs_rayon.json").write_text(json.dumps(rayon))
            (cost_dir / "runs_mpi.json").write_text(json.dumps(mpi))

            out = campaign_root / "report" / "lp"
            report_generators.render_cross_backend_table(campaign_root, "lp", out)

            table = (out / "cross_backend_table.md").read_text()
            self.assertIn("standalone", table)
            # At n=1M, best should be rayon-4 (30k ms) vs standalone (100k)
            row_1m = [l for l in table.splitlines() if "1,000,000" in l][0]
            self.assertIn("rayon-4", row_1m)


class CampaignSummaryTests(unittest.TestCase):
    def test_creates_readme_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            campaign_root = Path(td)
            metadata = {
                "created_at": "2026-05-23T10:00:00+00:00",
                "backends": ["standalone", "rayon", "burst"],
                "cost_sweep_nodes": [1000, 1_000_000],
                "size_nodes": [500_000, 1_000_000],
                "burst_partition_list": [4, 8],
                "rayon_threads": [1, 4, 8],
                "mpi_ranks": [4, 8],
                "mpi_hosts": "compute6,compute7",
            }
            (campaign_root / "metadata.json").write_text(json.dumps(metadata))
            # Create an algo report dir so README enumerates it
            (campaign_root / "report" / "lp").mkdir(parents=True)
            (campaign_root / "report" / "lp" / "cost_table.md").write_text("# stub")
            report_generators.render_campaign_summary(campaign_root, "lp")
            readme = (campaign_root / "report" / "README.md").read_text()
            self.assertIn("Backends:", readme)
            self.assertIn("standalone,rayon,burst", readme)
            self.assertIn("[lp](lp/)", readme)
            summary = (campaign_root / "report" / "summary.md").read_text()
            self.assertIn("Executive summary", summary)
            self.assertIn("[lp/cost_table.md](lp/cost_table.md)", summary)


if __name__ == "__main__":
    unittest.main()
