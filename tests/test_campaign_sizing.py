"""Tests for the per-cell resource sizing and fit-gate in cloudlab_common.

These guard the OOM-prevention logic that replaced the flat m=4096 stub:
memory now scales with graph size n, and every cell is checked against a single
worker node's budget BEFORE launch (turning a mid-run OOM into a fast skip).
"""
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
for sub in (ROOT, ROOT / "campaigns", ROOT / "infra"):
    if str(sub) not in sys.path:
        sys.path.insert(0, str(sub))

import cloudlab_common as c


class BurstMemorySizingTests(unittest.TestCase):
    def test_memory_scales_monotonically_with_n(self) -> None:
        sizes = [10_000, 100_000, 1_000_000, 5_000_000, 10_000_000]
        mems = [c.burst_memory_mb("lp", n, budget_mb=65536) for n in sizes]
        self.assertEqual(mems, sorted(mems), "memory must be non-decreasing in n")

    def test_small_n_uses_floor(self) -> None:
        self.assertEqual(c.burst_memory_mb("bfs", 10_000), c._BURST_MEM_FLOOR_MB)

    def test_large_n_gets_generous_tier(self) -> None:
        # n=10M previously OOM'd at 2048; must now be well above that.
        self.assertGreaterEqual(c.burst_memory_mb("lp", 10_000_000), 4096)

    def test_budget_clamp(self) -> None:
        # A tiny budget clamps the returned memory down to it.
        self.assertEqual(c.burst_memory_mb("lp", 10_000_000, budget_mb=3000), 3000)


class FitGateTests(unittest.TestCase):
    def test_p4_and_p8_fit_default_node(self) -> None:
        self.assertTrue(c.burst_cell_fit(partitions=4, granularity=2, memory_mb=4096)[0])
        self.assertTrue(c.burst_cell_fit(partitions=8, granularity=4, memory_mb=4096)[0])

    def test_p16_at_4g_is_blocked(self) -> None:
        # 16 × 4096 + reserved overflows a single 64 GiB node — must be caught.
        ok, request = c.burst_cell_fit(partitions=16, granularity=8, memory_mb=4096)
        self.assertFalse(ok)
        self.assertGreater(request.memory_mb, c.cloudlab_node_budget().usable_memory_mb)

    def test_spark_oversized_executor_blocked(self) -> None:
        self.assertFalse(c.spark_cell_fit(executors=8, executor_memory="12g")[0])

    def test_node_budget_reserves_headroom(self) -> None:
        budget = c.cloudlab_node_budget()
        self.assertLess(budget.usable_memory_mb, c.CLOUDLAB_NODE_TOTAL_MEMORY_MB)
        self.assertLess(budget.usable_cpus, c.CLOUDLAB_NODE_LOGICAL_CPUS)


if __name__ == "__main__":
    unittest.main()
