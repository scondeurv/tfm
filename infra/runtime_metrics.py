from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IterationWindow:
    index: int
    start_ms: int
    compute_end_ms: int | None
    reduce_end_ms: int | None
    broadcast_end_ms: int | None


def _flatten_worker_results(item: Any, workers: list[dict[str, Any]]) -> None:
    if isinstance(item, dict):
        workers.append(item)
        return
    if isinstance(item, list):
        for nested in item:
            _flatten_worker_results(nested, workers)


def unwrap_worker_results(results: list[Any] | None) -> list[dict[str, Any]]:
    workers: list[dict[str, Any]] = []
    for item in results or []:
        _flatten_worker_results(item, workers)
    return workers


def timestamp_map(worker_result: dict[str, Any]) -> dict[str, int]:
    mapped: dict[str, int] = {}
    for entry in worker_result.get("timestamps", []):
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        value = entry.get("value")
        if key is None or value is None:
            continue
        try:
            mapped[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return mapped


def _iter_window(iter_idx: int, worker_maps: list[dict[str, int]]) -> IterationWindow | None:
    start_key = f"iter_{iter_idx}_start"
    start_values = [mapping[start_key] for mapping in worker_maps if start_key in mapping]
    if not start_values:
        return None

    def phase_end(key: str) -> int | None:
        values = [mapping[key] for mapping in worker_maps if key in mapping]
        if not values:
            return None
        return max(values)

    return IterationWindow(
        index=iter_idx,
        start_ms=min(start_values),
        compute_end_ms=phase_end(f"iter_{iter_idx}_compute"),
        reduce_end_ms=phase_end(f"iter_{iter_idx}_reduce"),
        broadcast_end_ms=phase_end(f"iter_{iter_idx}_broadcast"),
    )


def collect_iteration_windows(worker_maps: list[dict[str, int]]) -> list[IterationWindow]:
    windows: list[IterationWindow] = []
    iter_idx = 0
    while True:
        window = _iter_window(iter_idx, worker_maps)
        if window is None:
            break
        windows.append(window)
        iter_idx += 1
    return windows


def compute_phase_breakdown(
    results: list[Any] | None,
    *,
    host_submit_ms: int | None = None,
    host_finished_ms: int | None = None,
) -> dict[str, Any]:
    worker_results = unwrap_worker_results(results)
    worker_maps = [timestamp_map(worker) for worker in worker_results]

    worker_starts = [mapping["worker_start"] for mapping in worker_maps if "worker_start" in mapping]
    worker_ends = [mapping["worker_end"] for mapping in worker_maps if "worker_end" in mapping]
    get_input_starts = [mapping["get_input"] for mapping in worker_maps if "get_input" in mapping]
    get_input_ends = [mapping["get_input_end"] for mapping in worker_maps if "get_input_end" in mapping]

    windows = collect_iteration_windows(worker_maps)

    compute_total_ms = 0
    reduce_total_ms = 0
    broadcast_total_ms = 0
    per_iteration: list[dict[str, int | None]] = []
    for window in windows:
        compute_ms = None
        reduce_ms = None
        broadcast_ms = None
        if window.compute_end_ms is not None:
            compute_ms = max(0, window.compute_end_ms - window.start_ms)
            compute_total_ms += compute_ms
        if window.compute_end_ms is not None and window.reduce_end_ms is not None:
            reduce_ms = max(0, window.reduce_end_ms - window.compute_end_ms)
            reduce_total_ms += reduce_ms
        if window.reduce_end_ms is not None and window.broadcast_end_ms is not None:
            broadcast_ms = max(0, window.broadcast_end_ms - window.reduce_end_ms)
            broadcast_total_ms += broadcast_ms
        per_iteration.append(
            {
                "iteration": window.index,
                "compute_ms": compute_ms,
                "reduce_ms": reduce_ms,
                "broadcast_ms": broadcast_ms,
            }
        )

    load_ms = None
    if get_input_starts and get_input_ends:
        load_ms = max(0, max(get_input_ends) - min(get_input_starts))
    elif worker_starts and get_input_ends:
        load_ms = max(0, max(get_input_ends) - min(worker_starts))

    cold_start_ms = None
    if host_submit_ms is not None and worker_starts:
        cold_start_ms = max(0, min(worker_starts) - host_submit_ms)

    host_total_ms = None
    if host_submit_ms is not None and host_finished_ms is not None:
        host_total_ms = max(0, host_finished_ms - host_submit_ms)

    warm_total_ms = None
    if worker_starts and worker_ends:
        warm_total_ms = max(0, max(worker_ends) - min(worker_starts))

    span_ms = None
    if worker_ends and windows:
        first_start = windows[0].start_ms
        span_ms = max(0, max(worker_ends) - first_start)
    elif worker_starts and worker_ends:
        span_ms = max(0, max(worker_ends) - min(worker_starts))

    stagger_ms = None
    if worker_starts:
        stagger_ms = max(0, max(worker_starts) - min(worker_starts))

    write_start_values = [
        value
        for mapping in worker_maps
        for key, value in mapping.items()
        if key.endswith("write_output_start") or key.endswith("write_labels_start")
    ]
    write_end_values = [
        value
        for mapping in worker_maps
        for key, value in mapping.items()
        if key.endswith("write_output_end") or key.endswith("write_labels_end")
    ]
    write_ms = None
    if write_start_values and write_end_values:
        write_ms = max(0, max(write_end_values) - min(write_start_values))

    return {
        "workers": len(worker_results),
        "cold_start_ms": cold_start_ms,
        "stagger_ms": stagger_ms,
        "load_ms": load_ms,
        "compute_ms": compute_total_ms if windows else None,
        "reduce_ms": reduce_total_ms if windows else None,
        "broadcast_ms": broadcast_total_ms if windows else None,
        "communication_ms": (reduce_total_ms + broadcast_total_ms) if windows else None,
        "warm_total_ms": warm_total_ms,
        "host_total_ms": host_total_ms,
        "span_ms": span_ms,
        "write_ms": write_ms,
        "iterations": len(windows),
        "per_iteration": per_iteration,
    }


def estimate_logical_traffic_bytes(
    *,
    algorithm: str,
    num_nodes: int,
    workers: int,
    iterations: int,
) -> dict[str, int] | None:
    if workers <= 1 or iterations <= 0:
        return {
            "reduce_bytes": 0,
            "broadcast_bytes": 0,
            "total_bytes": 0,
        }

    if algorithm == "sssp":
        message_bytes = (num_nodes + 1) * 4
        per_round = (workers - 1) * message_bytes
        return {
            "reduce_bytes": per_round * iterations,
            "broadcast_bytes": per_round * iterations,
            "total_bytes": 2 * per_round * iterations,
        }

    if algorithm == "labelpropagation":
        seed_sync_bytes = 2 * (workers - 1) * 4
        initial_labels_bytes = 2 * (workers - 1) * (num_nodes * 4)
        loop_message_bytes = 2 * (workers - 1) * ((num_nodes + 1) * 4) * iterations
        return {
            "reduce_bytes": (workers - 1) * ((num_nodes + 1) * 4) * iterations + (workers - 1) * 4 + (workers - 1) * (num_nodes * 4),
            "broadcast_bytes": (workers - 1) * ((num_nodes + 1) * 4) * iterations + (workers - 1) * 4 + (workers - 1) * (num_nodes * 4),
            "total_bytes": seed_sync_bytes + initial_labels_bytes + loop_message_bytes,
        }

    return None
