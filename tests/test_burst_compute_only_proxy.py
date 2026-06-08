"""Unit + e2e tests for Burst compute_only proxy extraction in the orchestrator.

Targets `campaigns/run_cloudlab_campaign.py`:
  - `extract_burst_compute_only_proxy(log_path)`
  - `apply_compute_only_proxy(record, log_path)`

These cover the case where the redis-list Burst backend (BFS/SSSP) does not
emit host-side `compute_only_ms`, and the orchestrator must fall back to
parsing `activation.duration` from invocation logs.
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

from run_cloudlab_campaign import (  # noqa: E402
    apply_compute_only_proxy,
    extract_burst_compute_only_proxy,
)


def _write_log(tmp: Path, body: str) -> Path:
    log = tmp / "burst.log"
    log.write_text(body, encoding="utf-8")
    return log


# A representative slice of an OpenWhisk activation record as it appears in
# benchmark_bfs.py invocation logs. Two activations (one per worker) for
# n=1M p=4. The slowest worker duration is 2266 ms.
_SAMPLE_ACTIVATIONS = textwrap.dedent("""
    2026-05-05 14:23:03 - DEBUG - {'activationId': 'a1', 'duration': 2266, 'end': 1777990982023, 'start': 1777990979757}
    2026-05-05 14:23:03 - DEBUG - {'activationId': 'a2', 'duration': 2077, 'end': 1777990981883, 'start': 1777990979806}
""").strip()


class ExtractBurstComputeOnlyProxyTests(unittest.TestCase):
    def test_returns_max_duration_across_activations(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), _SAMPLE_ACTIVATIONS)
            self.assertEqual(extract_burst_compute_only_proxy(log), 2266)

    def test_returns_none_when_log_has_no_activations(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), "Burst cluster already clean\nRunning burst BFS...\n")
            self.assertIsNone(extract_burst_compute_only_proxy(log))

    def test_returns_none_when_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self.assertIsNone(extract_burst_compute_only_proxy(Path(td) / "missing.log"))

    def test_handles_single_worker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            body = "'duration': 1500, 'end': 1777990000000"
            log = _write_log(Path(td), body)
            self.assertEqual(extract_burst_compute_only_proxy(log), 1500)

    def test_ignores_unrelated_duration_keys(self) -> None:
        # Sibling 'duration' fields without an adjacent 'end' should not match.
        body = "config: {'duration_threshold': 9999} ; 'duration': 100, 'end': 1"
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), body)
            self.assertEqual(extract_burst_compute_only_proxy(log), 100)


class ApplyComputeOnlyProxyTests(unittest.TestCase):
    def _record_without_compute_only(self) -> dict:
        # Mirrors the BFS/SSSP redis-list shape: compute_only_ms is None and
        # phase_metrics fields are mostly null. host_total_ms / e2e populated.
        return {
            "phase": "size_sweep",
            "framework": "burst",
            "algorithm": "bfs",
            "nodes": 1_000_000,
            "partitions": 4,
            "status": "passed",
            "result": {
                "burst": {
                    "compute_only_ms": None,
                    "end_to_end_ms": 13787,
                    "host_total_time_ms": 13787,
                    "phase_metrics": {
                        "compute_ms": None,
                        "host_total_ms": 13787,
                        "workers": 2,
                    },
                },
            },
        }

    def test_injects_proxy_and_backfills_compute_only_ms(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), _SAMPLE_ACTIVATIONS)
            rec = self._record_without_compute_only()
            apply_compute_only_proxy(rec, log)
            burst = rec["result"]["burst"]
            self.assertEqual(burst["compute_only_ms_proxy"], 2266)
            self.assertEqual(burst["compute_only_ms_proxy_source"], "max_activation_duration")
            self.assertEqual(burst["compute_only_ms"], 2266)
            self.assertEqual(burst["phase_metrics"]["compute_ms"], 2266)

    def test_does_not_overwrite_existing_compute_only_ms(self) -> None:
        # LP backend exposes compute_only_ms directly; proxy must not clobber it.
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), _SAMPLE_ACTIVATIONS)
            rec = self._record_without_compute_only()
            rec["result"]["burst"]["compute_only_ms"] = 4328
            rec["result"]["burst"]["phase_metrics"]["compute_ms"] = 2769
            apply_compute_only_proxy(rec, log)
            burst = rec["result"]["burst"]
            self.assertEqual(burst["compute_only_ms"], 4328)
            self.assertEqual(burst["phase_metrics"]["compute_ms"], 2769)
            # Proxy still recorded for diagnostics.
            self.assertEqual(burst["compute_only_ms_proxy"], 2266)

    def test_noop_when_log_unparseable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), "no activations here\n")
            rec = self._record_without_compute_only()
            apply_compute_only_proxy(rec, log)
            burst = rec["result"]["burst"]
            self.assertIsNone(burst["compute_only_ms"])
            self.assertNotIn("compute_only_ms_proxy", burst)

    def test_noop_when_burst_field_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log = _write_log(Path(td), _SAMPLE_ACTIVATIONS)
            rec = {"result": {}}
            apply_compute_only_proxy(rec, log)
            self.assertEqual(rec, {"result": {}})


class EndToEndRehydrationTests(unittest.TestCase):
    """Simulate the orchestrator's cached-record code path:

    A raw_runs JSON file already exists from a prior run with null
    compute_only_ms; the orchestrator re-reads it, finds the matching log,
    and writes back the record with the proxy injected.
    """

    def test_rehydrates_cached_record_from_log(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            log = _write_log(tmp, _SAMPLE_ACTIVATIONS)
            raw = tmp / "raw.json"
            cached = {
                "phase": "size_sweep",
                "framework": "burst",
                "algorithm": "sssp",
                "nodes": 500_000,
                "partitions": 4,
                "status": "passed",
                "result": {
                    "burst": {
                        "compute_only_ms": None,
                        "end_to_end_ms": 12793,
                        "phase_metrics": {"compute_ms": None, "host_total_ms": 12793},
                    },
                },
            }
            raw.write_text(json.dumps(cached))

            # Mimic orchestrator's cached-branch behaviour.
            record = json.loads(raw.read_text())
            apply_compute_only_proxy(record, log)
            raw.write_text(json.dumps(record))

            written = json.loads(raw.read_text())
            burst = written["result"]["burst"]
            self.assertEqual(burst["compute_only_ms"], 2266)
            self.assertEqual(burst["compute_only_ms_proxy"], 2266)
            self.assertEqual(burst["phase_metrics"]["compute_ms"], 2266)


if __name__ == "__main__":
    unittest.main()
