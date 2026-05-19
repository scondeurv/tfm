from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
LABELPROP = ROOT / "labelpropagation"
if str(LABELPROP) not in sys.path:
    sys.path.insert(0, str(LABELPROP))

from runtime_metrics import estimate_logical_traffic_bytes, estimate_wcc_phase_metrics


class CanonicalMetricTests(unittest.TestCase):
    def test_bfs_logical_traffic_is_estimated(self) -> None:
        traffic = estimate_logical_traffic_bytes(
            algorithm="bfs",
            num_nodes=1000,
            workers=4,
            iterations=3,
        )
        self.assertIsNotNone(traffic)
        self.assertEqual(traffic["reduce_bytes"], traffic["broadcast_bytes"])
        self.assertGreater(traffic["total_bytes"], 0)

    def test_wcc_logical_traffic_uses_minimum_one_round(self) -> None:
        traffic = estimate_logical_traffic_bytes(
            algorithm="wcc",
            num_nodes=1000,
            workers=4,
            iterations=0,
        )
        self.assertIsNotNone(traffic)
        self.assertGreater(traffic["total_bytes"], 0)

    def test_estimated_wcc_phase_metrics_follow_canonical_schema(self) -> None:
        metrics = estimate_wcc_phase_metrics(
            {
                "cold_start_ms": 1200,
                "stagger_ms": 150,
                "computation_ms": 900,
                "warm_total_ms": 1100,
                "total_ms": 2300,
            },
            workers=8,
        )
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics["workers"], 8)
        self.assertEqual(metrics["compute_ms"], 900)
        self.assertEqual(metrics["communication_ms"], 200)
        self.assertEqual(metrics["warm_total_ms"], 1100)
        self.assertEqual(metrics["host_total_ms"], 2300)
        self.assertEqual(metrics["iterations"], 1)
        self.assertTrue(metrics["estimated"])


if __name__ == "__main__":
    unittest.main()
