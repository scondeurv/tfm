from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
LABELPROP = ROOT / "labelpropagation"
if str(LABELPROP) not in sys.path:
    sys.path.insert(0, str(LABELPROP))

from runtime_metrics import estimate_logical_traffic_bytes


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


if __name__ == "__main__":
    unittest.main()
