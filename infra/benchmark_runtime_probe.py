#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_client.time_helper import get_millis

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.append(str(HERE))

from runtime_metrics import timestamp_map, unwrap_worker_results

BENCHMARK_JSON_PREFIX = "BENCHMARK_RESULT_JSON:"
ZIP_PATH = Path(__file__).resolve().with_name("runtime_probe.zip")


def probe_payloads(
    *,
    mode: str,
    workers: int,
    granularity: int,
    payload_bytes: int,
    iterations: int,
    bucket: str | None = None,
    key_prefix: str | None = None,
    region: str | None = None,
    endpoint: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
) -> list[dict]:
    partitions = workers
    return [
        {
            "mode": mode,
            "partitions": partitions,
            "granularity": granularity,
            "payload_bytes": payload_bytes,
            "iterations": iterations,
            "root_worker": 0,
            "peer_worker": 1,
            "bucket": bucket,
            "key_prefix": key_prefix,
            "region": region,
            "endpoint": endpoint,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
        }
        for _ in range(workers)
    ]


def startup_summary(results: list[dict], host_submit: int, host_finished: int) -> dict:
    worker_maps = [timestamp_map(worker) for worker in results]
    starts = sorted(mapping["worker_start"] for mapping in worker_maps if "worker_start" in mapping)
    latencies = [start - host_submit for start in starts]
    offsets = [start - starts[0] for start in starts] if starts else []
    return {
        "host_total_ms": host_finished - host_submit,
        "startup_latency_ms": latencies,
        "startup_median_ms": sorted(latencies)[len(latencies) // 2] if latencies else None,
        "stagger_ms": max(offsets) if offsets else None,
        "simultaneity_offsets_ms": offsets,
    }


def collective_summary(results: list[dict], mode: str, payload_bytes: int, iterations: int) -> dict:
    worker_maps = [timestamp_map(worker) for worker in results]
    latencies = []
    suffix = "broadcast_end" if mode == "broadcast" else "all_to_all_end"
    for iter_idx in range(iterations):
        start_key = f"iter_{iter_idx}_start"
        end_key = f"iter_{iter_idx}_{suffix}"
        starts = [mapping[start_key] for mapping in worker_maps if start_key in mapping]
        ends = [mapping[end_key] for mapping in worker_maps if end_key in mapping]
        if starts and ends:
            latencies.append(max(ends) - min(starts))
    total_bytes = payload_bytes if mode == "broadcast" else payload_bytes * len(results)
    return {
        "latency_ms": latencies,
        "latency_median_ms": sorted(latencies)[len(latencies) // 2] if latencies else None,
        "payload_bytes_per_collective": total_bytes,
        "iterations": iterations,
    }


def ptp_summary(results: list[dict], payload_bytes: int, iterations: int, aggregate_pairs: bool = False) -> dict:
    worker_maps = {worker.get("worker_id"): timestamp_map(worker) for worker in results}
    if not aggregate_pairs:
        root = worker_maps.get(0, {})
        if "ptp_start" not in root or "ptp_end" not in root:
            return {
                "throughput_mb_s": None,
                "duration_ms": None,
                "payload_bytes": payload_bytes,
                "iterations": iterations,
            }
        duration_ms = max(0, root["ptp_end"] - root["ptp_start"])
        total_bytes = payload_bytes * iterations
        throughput_mb_s = None
        if duration_ms > 0:
            throughput_mb_s = round(total_bytes / (duration_ms / 1000.0) / (1024 * 1024), 4)
        return {
            "throughput_mb_s": throughput_mb_s,
            "duration_ms": duration_ms,
            "payload_bytes": payload_bytes,
            "iterations": iterations,
            "total_bytes": total_bytes,
        }

    starts = [mapping["ptp_start"] for mapping in worker_maps.values() if "ptp_start" in mapping]
    ends = [mapping["ptp_end"] for mapping in worker_maps.values() if "ptp_end" in mapping]
    if not starts or not ends:
        return {
            "throughput_mb_s": None,
            "duration_ms": None,
            "payload_bytes": payload_bytes,
            "iterations": iterations,
            "pair_count": 0,
        }
    duration_ms = max(0, max(ends) - min(starts))
    pair_count = len(starts) // 2
    total_bytes = payload_bytes * iterations * pair_count
    throughput_mb_s = None
    if duration_ms > 0:
        throughput_mb_s = round(total_bytes / (duration_ms / 1000.0) / (1024 * 1024), 4)
    return {
        "throughput_mb_s": throughput_mb_s,
        "duration_ms": duration_ms,
        "payload_bytes": payload_bytes,
        "iterations": iterations,
        "total_bytes": total_bytes,
        "pair_count": pair_count,
    }


def load_summary(results: list[dict]) -> dict:
    worker_maps = [timestamp_map(worker) for worker in results]
    starts = [mapping["get_input"] for mapping in worker_maps if "get_input" in mapping]
    ends = [mapping["get_input_end"] for mapping in worker_maps if "get_input_end" in mapping]
    load_ms = None
    if starts and ends:
        load_ms = max(0, max(ends) - min(starts))
    bytes_per_worker = [
        int(worker.get("bytes_read", 0))
        for worker in results
        if worker.get("bytes_read") is not None
    ]
    return {
        "load_ms": load_ms,
        "bytes_total": sum(bytes_per_worker) if bytes_per_worker else None,
        "bytes_per_worker": bytes_per_worker,
    }


def run_probe(args: argparse.Namespace) -> dict:
    if not ZIP_PATH.exists():
        raise SystemExit(
            f"Missing {ZIP_PATH}. Compile the probe first with ./compile_runtime_probe_cluster.sh"
        )

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, debug=True)
    payloads = probe_payloads(
        mode=args.mode,
        workers=args.workers,
        granularity=args.granularity,
        payload_bytes=args.payload_bytes,
        iterations=args.iterations,
        bucket=args.bucket,
        key_prefix=args.key_prefix,
        region=args.region,
        endpoint=args.s3_endpoint,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        aws_session_token=args.aws_session_token,
    )
    host_submit = get_millis()
    dataset = executor.burst(
        "runtime-probe",
        payloads,
        file=str(ZIP_PATH),
        is_zip=True,
        memory=args.memory,
        custom_image="burstcomputing/runtime-rust-burst:latest",
        debug_mode=True,
        backend=args.backend,
        chunk_size=args.chunk_size,
        granularity=args.granularity,
        join=False,
        timeout=900000,
    )
    host_finished = get_millis()
    results = unwrap_worker_results(dataset.get_results())
    for worker in results:
        if worker.get("summary"):
            print(worker["summary"])

    if args.mode == "startup":
        metrics = startup_summary(results, host_submit, host_finished)
    elif args.mode == "load":
        metrics = load_summary(results)
    elif args.mode in {"broadcast", "all_to_all"}:
        metrics = collective_summary(results, args.mode, args.payload_bytes, args.iterations)
    elif args.mode == "ptp_pairs":
        metrics = ptp_summary(results, args.payload_bytes, args.iterations, aggregate_pairs=True)
    else:
        metrics = ptp_summary(results, args.payload_bytes, args.iterations)

    return {
        "probe": args.mode,
        "configuration": {
            "workers": args.workers,
            "granularity": args.granularity,
            "partitions": args.workers,
            "memory_mb": args.memory,
            "payload_bytes": args.payload_bytes,
            "iterations": args.iterations,
            "backend": args.backend,
            "chunk_size": args.chunk_size,
            "bucket": args.bucket,
            "key_prefix": args.key_prefix,
        },
        "metrics": metrics,
        "workers": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the generic Burst probe used by the canonical campaign.")
    parser.add_argument("--mode", choices=("startup", "load", "broadcast", "all_to_all", "ptp", "ptp_pairs"), required=True)
    parser.add_argument("--workers", type=int, required=True, help="Logical workers in the burst")
    parser.add_argument("--granularity", type=int, default=1, help="Workers per Burst pack")
    parser.add_argument("--payload-bytes", type=int, default=1048576, help="Payload size per message")
    parser.add_argument("--iterations", type=int, default=8, help="Collective iterations")
    parser.add_argument("--memory", type=int, default=1024, help="Memory per worker in MB")
    parser.add_argument("--backend", default="redis-list", help="Burst communication backend")
    parser.add_argument("--chunk-size", type=int, default=1024, help="Burst middleware chunk size in KB")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "test-bucket"))
    parser.add_argument("--key-prefix", default=None)
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_WORKER_ENDPOINT", "http://minio-service.default.svc.cluster.local:9000"))
    parser.add_argument("--aws-access-key-id", default=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
    parser.add_argument("--aws-secret-access-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--aws-session-token", default=os.environ.get("AWS_SESSION_TOKEN"))
    parser.add_argument("--ow-host", default=os.environ.get("OW_HOST", "localhost"))
    parser.add_argument("--ow-port", type=int, default=int(os.environ.get("OW_PORT", "31001")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_probe(args)
    print(f"{BENCHMARK_JSON_PREFIX}{json.dumps(summary, sort_keys=True)}")


if __name__ == "__main__":
    main()
