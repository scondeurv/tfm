from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
LABELPROP = ROOT / "labelpropagation"
if str(LABELPROP) not in sys.path:
    sys.path.insert(0, str(LABELPROP))

from runtime_metrics import compute_phase_breakdown, unwrap_worker_results


def ts(key: str, value: int) -> dict:
    return {"key": key, "value": str(value)}


class Atc25MetricsTests(unittest.TestCase):
    def test_unwrap_worker_results_flattens_nested_burst_results(self) -> None:
        raw_results = [
            [
                {"worker_id": 0, "timestamps": [ts("worker_start", 10)]},
                {"worker_id": 1, "timestamps": [ts("worker_start", 11)]},
            ],
            [[{"worker_id": 2, "timestamps": [ts("worker_start", 12)]}]],
        ]

        flattened = unwrap_worker_results(raw_results)

        self.assertEqual([worker["worker_id"] for worker in flattened], [0, 1, 2])

    def test_compute_phase_breakdown_uses_all_nested_workers(self) -> None:
        raw_results = [
            [
                {
                    "worker_id": 0,
                    "timestamps": [
                        ts("worker_start", 100),
                        ts("get_input", 110),
                        ts("get_input_end", 140),
                        ts("iter_0_start", 150),
                        ts("iter_0_compute", 170),
                        ts("iter_0_reduce", 180),
                        ts("iter_0_broadcast", 190),
                        ts("worker_end", 220),
                    ],
                },
                {
                    "worker_id": 1,
                    "timestamps": [
                        ts("worker_start", 105),
                        ts("get_input", 112),
                        ts("get_input_end", 145),
                        ts("iter_0_start", 151),
                        ts("iter_0_compute", 168),
                        ts("iter_0_reduce", 181),
                        ts("iter_0_broadcast", 191),
                        ts("worker_end", 215),
                    ],
                },
            ]
        ]

        phase = compute_phase_breakdown(raw_results, host_submit_ms=90, host_finished_ms=230)

        self.assertEqual(phase["workers"], 2)
        self.assertEqual(phase["cold_start_ms"], 10)
        self.assertEqual(phase["load_ms"], 35)
        self.assertEqual(phase["compute_ms"], 20)
        self.assertEqual(phase["reduce_ms"], 11)
        self.assertEqual(phase["broadcast_ms"], 10)
        self.assertEqual(phase["communication_ms"], 21)
        self.assertEqual(phase["warm_total_ms"], 120)
        self.assertEqual(phase["span_ms"], 70)


if __name__ == "__main__":
    unittest.main()
