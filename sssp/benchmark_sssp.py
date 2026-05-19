#!/usr/bin/env python3
"""Compare standalone SSSP with the Burst implementation."""
import argparse
import json
import subprocess
import sys
import os
from pathlib import Path


import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_client.time_helper import get_millis
from sssp_utils import generate_sssp_payload

ROOT = Path(__file__).resolve().parents[1]
LP_DIR = ROOT / "labelpropagation"
if str(LP_DIR) not in sys.path:
    sys.path.append(str(LP_DIR))

from runtime_metrics import compute_phase_breakdown, estimate_logical_traffic_bytes

DEFAULT_WORKER_S3_ENDPOINT = os.environ.get("S3_WORKER_ENDPOINT", "http://minio-service.default.svc.cluster.local:9000")
DEFAULT_HOST_S3_ENDPOINT = os.environ.get("S3_HOST_ENDPOINT", "http://localhost:9000")
BENCHMARK_JSON_PREFIX = "BENCHMARK_RESULT_JSON:"
CLEAN_BURST_CLUSTER_SCRIPT = ROOT / "clean_burst_cluster.sh"


def clean_burst_cluster() -> None:
    result = subprocess.run(
        ["bash", str(CLEAN_BURST_CLUSTER_SCRIPT)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError("failed to clean Burst cluster before running sssp")

STANDALONE_BINARY = "sssp-standalone/target/release/sssp-standalone"


def benchmark_standalone(graph_file: str, num_nodes: int, source_node: int, max_iterations: int):
    """Run the standalone SSSP binary and return the full JSON output dict."""
    if not os.path.exists(STANDALONE_BINARY):
        print(
            f"Error: Binary not found at {STANDALONE_BINARY}\n"
            "Run: cd sssp-standalone && cargo build --release",
            file=sys.stderr,
        )
        return None

    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None

    try:
        result = subprocess.run(
            [STANDALONE_BINARY, graph_file, str(num_nodes), str(source_node), str(max_iterations)],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            print(f"Error running standalone SSSP: {result.stderr}", file=sys.stderr)
            return None

        output = json.loads(result.stdout.strip())
        return output  # full dict: load_time_ms, execution_time_ms, total_time_ms, reachable_nodes, max_distance, distances

    except subprocess.TimeoutExpired:
        print("Error: Standalone SSSP timed out", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing standalone output: {e}", file=sys.stderr)
        print(f"Output was: {result.stdout[:500]}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return None


def _is_loopback_endpoint(endpoint: str) -> bool:
    """Return True when endpoint host points to localhost/loopback."""
    raw = (endpoint or "").strip().lower()
    if "://" not in raw:
        raw = f"http://{raw}"
    host_port = raw.split("://", 1)[1].split("/", 1)[0]
    host = host_port.split("@")[-1].split(":", 1)[0].strip("[]")
    return host in {"localhost", "127.0.0.1", "::1"}


def benchmark_burst(
    num_nodes: int,
    num_partitions: int,
    source_node: int,
    max_iterations: int,
    memory_mb: int,
    granularity: int = 1,
    ow_host: str = "localhost",
    ow_port: int = 31001,
    s3_endpoint: str = "http://minio-service.default.svc.cluster.local:9000",
    validation_endpoint: str = "http://localhost:9000",
    bucket: str = "test-bucket",
    key_prefix: str = "graphs",
    backend: str = "redis-list",
    chunk_size: int = 1024,
):
    """Run the distributed SSSP burst action and return phase-aware metrics."""
    if _is_loopback_endpoint(s3_endpoint):
        print(
            "Error: Invalid worker S3 endpoint for burst workers: "
            f"{s3_endpoint}. Use --s3-endpoint with the internal cluster endpoint "
            "and --validation-endpoint for host preflight access.",
            file=sys.stderr,
        )
        return None, None, None, None, None

    s3_prefix = f"{key_prefix}/large-sssp-{num_nodes}"

    ok, preflight_error = preflight_sssp_input(
        num_nodes=num_nodes,
        num_partitions=num_partitions,
        validation_endpoint=validation_endpoint,
        bucket=bucket,
        s3_prefix=s3_prefix,
    )
    if not ok:
        print(preflight_error, file=sys.stderr)
        return None, None, None, None, None

    params = generate_sssp_payload(
        endpoint=s3_endpoint,
        partitions=num_partitions,
        num_nodes=num_nodes,
        bucket=bucket,
        key=s3_prefix,
        source_node=source_node,
        max_iterations=max_iterations,
        granularity=granularity,
    )

    executor = OpenwhiskExecutor(ow_host, ow_port, debug=True)

    try:
        host_submit = get_millis()
        dt = executor.burst(
            "sssp",
            params,
            file="./sssp.zip",
            memory=memory_mb,
            custom_image="burstcomputing/runtime-rust-burst:latest",
            debug_mode=True,
            granularity=granularity,
            join=False,
            backend=backend,
            chunk_size=chunk_size,
            is_zip=True,
            timeout=900000,
        )
        finished = get_millis()

        results = dt.get_results()
        if not results:
            print("Error: No results from burst SSSP", file=sys.stderr)
            return None, None, None, None, None

        for r in results:
            worker_data = r[0] if isinstance(r, list) and r else r
            if isinstance(worker_data, dict) and worker_data.get("results"):
                print(worker_data["results"])

        phase_metrics = compute_phase_breakdown(
            results,
            host_submit_ms=host_submit,
            host_finished_ms=finished,
        )

        return (
            finished - host_submit,
            phase_metrics.get("warm_total_ms"),
            phase_metrics.get("span_ms"),
            results,
            phase_metrics,
        )

    except Exception as e:
        print(f"Error running burst SSSP: {e}", file=sys.stderr)
        return None, None, None, None, None


def download_burst_distances(endpoint: str, bucket: str, key: str) -> dict | None:
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        obj = s3.get_object(Bucket=bucket, Key=f"{key}/output/sssp_distances_final.json")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as exc:
        print(f"Validation: could not fetch burst SSSP distances: {exc}", file=sys.stderr)
        return None


def _extract_report_stat(report: str, label: str, cast):
    for line in report.splitlines():
        if label not in line:
            continue
        try:
            return cast(line.split(":", 1)[1].strip().split()[0])
        except Exception:
            return None
    return None


def run_validation(
    standalone_output: dict,
    burst_results: list,
    num_nodes: int,
    endpoint: str,
    bucket: str,
    key: str,
) -> bool:
    """Compare the full distance vector between standalone and burst."""
    if not standalone_output or not burst_results:
        print("Validation: missing data", file=sys.stderr)
        return False

    sa_reachable = standalone_output.get("reachable_nodes", -1)
    sa_max_dist = standalone_output.get("max_distance", -1.0)
    sa_distances = standalone_output.get("distances")
    burst_payload = download_burst_distances(endpoint, bucket, key)
    if burst_payload and isinstance(sa_distances, list):
        burst_distances = burst_payload.get("distances")
        if not isinstance(burst_distances, list):
            print("Validation: burst distances payload is missing distances", file=sys.stderr)
            return False
        if len(sa_distances) != len(burst_distances):
            print(
                f"  ✗ distances length mismatch: standalone={len(sa_distances)}, burst={len(burst_distances)}",
                file=sys.stderr,
            )
            return False
        for idx, (sa, bs) in enumerate(zip(sa_distances, burst_distances)):
            if sa == bs:
                continue
            if isinstance(sa, (int, float)) and isinstance(bs, (int, float)):
                if sa == float("inf") and bs == float("inf"):
                    continue
                tol = max(abs(sa) * 1e-4, 1e-3)
                if abs(sa - bs) <= tol:
                    continue
            print(
                f"  ✗ distance mismatch at node {idx}: standalone={sa}, burst={bs}",
                file=sys.stderr,
            )
            return False
        print(f"  ✓ Full distance vector matches for {len(sa_distances)} nodes")
        return True

    burst_report = None
    for r in burst_results:
        if isinstance(r, list) and len(r) > 0:
            r = r[0]
        if isinstance(r, dict) and r.get("results"):
            burst_report = r["results"]
            break

    if burst_report is None:
        print("Validation: could not find burst root worker output", file=sys.stderr)
        return False

    burst_reachable = _extract_report_stat(burst_report, "Reachable nodes:", int)
    burst_max_dist = _extract_report_stat(burst_report, "Max distance:", float)

    ok = True
    if burst_reachable is not None and sa_reachable != burst_reachable:
        print(
            f"  ✗ reachable_nodes mismatch: standalone={sa_reachable}, burst={burst_reachable}",
            file=sys.stderr,
        )
        ok = False
    if burst_max_dist is not None and sa_max_dist >= 0:
        tol = max(abs(sa_max_dist) * 1e-4, 1e-3)
        if abs(sa_max_dist - burst_max_dist) > tol:
            print(
                f"  ✗ max_distance mismatch: standalone={sa_max_dist:.4f}, burst={burst_max_dist:.4f} (tol={tol:.4f})",
                file=sys.stderr,
            )
            ok = False

    if ok:
        print(
            f"  ✓ reachable_nodes={sa_reachable}, max_distance={sa_max_dist:.4f} — MATCH"
        )
    return ok


def pick_winner(speedup: float | None) -> str | None:
    if speedup is None:
        return None
    return "Burst" if speedup > 1.0 else "Standalone"


def preflight_sssp_input(num_nodes: int, num_partitions: int, validation_endpoint: str, bucket: str, s3_prefix: str):
    """Ensure the Burst input exists in S3 before invoking workers."""
    endpoint = validation_endpoint if validation_endpoint.startswith("http") else f"http://{validation_endpoint}"
    partition_prefix = f"{s3_prefix}/"
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    try:
        missing = []
        for part_id in range(num_partitions):
            key = f"{s3_prefix}/part-{part_id:05d}"
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in {"404", "NoSuchKey", "NotFound"}:
                    missing.append(key)
                    continue
                raise
        if not missing:
            return True, None
        generation_cmd = f"python3 setup_large_sssp_data.py --nodes {num_nodes} --partitions {num_partitions} --bucket {bucket} --endpoint {endpoint}"
        message = (
            "Error: Missing or empty SSSP burst input in S3.\n"
            f"Checked endpoint={endpoint}, bucket={bucket}.\n"
            f"Expected exact partition keys under: {partition_prefix}\n"
            f"Missing partition objects: {missing[:10]}"
            + (f" ... ({len(missing)} total)" if len(missing) > 10 else "")
            + "\n"
            "Generate data first with:\n"
            f"  {generation_cmd}"
        )
        return False, message
    except (BotoCoreError, ClientError) as exc:
        return False, (
            "Error: S3 preflight failed before burst invocation.\n"
            f"Endpoint={endpoint}, bucket={bucket}, partition prefix={partition_prefix}.\n"
            f"S3 error: {exc}"
        )


def build_benchmark_summary(
    nodes: int,
    source_node: int,
    max_iterations: int,
    partitions: int,
    granularity: int,
    memory_mb: int,
    standalone_output: dict | None,
    burst_host_total_ms: float | None,
    burst_warm_total_ms: float | None,
    burst_algo_ms: float | None,
    validation_requested: bool,
    validation_performed: bool,
    validation_passed: bool | None,
    validation_skipped_reason: str | None,
    validation_mode: str | None,
    key_prefix: str,
    phase_metrics: dict | None,
    backend: str,
    chunk_size: int,
) -> dict:
    standalone_exec_ms = None
    standalone_total_ms = None
    if standalone_output is not None:
        standalone_exec_ms = standalone_output.get("execution_time_ms")
        standalone_total_ms = standalone_output.get("total_time_ms")

    algo_speedup = None
    warm_speedup = None
    cold_speedup = None
    if standalone_exec_ms not in (None, 0) and burst_algo_ms not in (None, 0):
        algo_speedup = standalone_exec_ms / burst_algo_ms
    if standalone_total_ms not in (None, 0) and burst_warm_total_ms not in (None, 0):
        warm_speedup = standalone_total_ms / burst_warm_total_ms
    if standalone_total_ms not in (None, 0) and burst_host_total_ms not in (None, 0):
        cold_speedup = standalone_total_ms / burst_host_total_ms

    primary_metric = "total" if cold_speedup is not None else ("warm" if warm_speedup is not None else "span")
    primary_winner = (
        pick_winner(cold_speedup)
        if primary_metric == "total"
        else pick_winner(warm_speedup)
        if primary_metric == "warm"
        else pick_winner(algo_speedup)
    )
    workers = partitions
    traffic = estimate_logical_traffic_bytes(
        algorithm="sssp",
        num_nodes=nodes,
        workers=workers,
        iterations=(phase_metrics or {}).get("iterations", 0) or 0,
    )

    return {
        "algorithm": "sssp",
        "dataset": {
            "nodes": nodes,
            "graph_file": f"large_sssp_{nodes}.txt",
            "s3_prefix": f"{key_prefix}/large-sssp-{nodes}",
        },
        "configuration": {
            "source_node": source_node,
            "max_iterations": max_iterations,
            "partitions": partitions,
            "granularity": granularity,
            "memory_mb": memory_mb,
            "backend": backend,
            "chunk_size": chunk_size,
        },
        "standalone": {
            "compute_only_ms": standalone_exec_ms,
            "execution_time_ms": standalone_exec_ms,
            "end_to_end_ms": standalone_total_ms,
            "total_time_ms": standalone_total_ms,
            "reachable_nodes": standalone_output.get("reachable_nodes") if standalone_output else None,
            "max_distance": standalone_output.get("max_distance") if standalone_output else None,
        },
        "burst": {
            "compute_only_ms": burst_algo_ms,
            "processing_time_ms": burst_algo_ms,
            "warm_total_ms": burst_warm_total_ms,
            "total_time_ms": burst_warm_total_ms,
            "end_to_end_ms": burst_host_total_ms,
            "host_total_time_ms": burst_host_total_ms,
            "output_write_ms": (phase_metrics or {}).get("write_ms") if phase_metrics else None,
            "phase_metrics": phase_metrics,
            "logical_traffic_bytes": traffic,
        },
        "speedup": {
            "compute_only": algo_speedup,
            "algorithmic": algo_speedup,
            "warm_total": warm_speedup,
            "end_to_end": cold_speedup,
            "cold_total": cold_speedup,
            "overall": cold_speedup if cold_speedup is not None else warm_speedup,
        },
        "winner": {
            "span": pick_winner(algo_speedup),
            "warm": pick_winner(warm_speedup),
            "total": pick_winner(cold_speedup),
            "primary_metric": primary_metric,
            "primary": primary_winner,
        },
        "validation": {
            "requested": validation_requested,
            "performed": validation_performed,
            "passed": validation_passed,
            "skipped_reason": validation_skipped_reason,
            "mode": validation_mode if validation_performed else None,
        },
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark SSSP: Standalone vs Burst")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--partitions", type=int, default=8, help="S3 partitions for burst")
    parser.add_argument("--granularity", type=int, default=1, help="Workers per Burst pack")
    parser.add_argument("--max-iterations", type=int, default=500, help="Maximum relaxation iterations")
    parser.add_argument("--source", type=int, default=0, help="SSSP source node")
    parser.add_argument("--memory", type=int, default=512, help="Memory per worker (MB)")
    parser.add_argument("--ow-host", type=str, default="localhost")
    parser.add_argument("--ow-port", type=int, default=31001)
    parser.add_argument("--skip-standalone", action="store_true")
    parser.add_argument("--skip-burst", action="store_true")
    parser.add_argument("--backend", default="redis-list", help="Burst communication backend")
    parser.add_argument("--chunk-size", type=int, default=1024, help="Burst middleware chunk size in KB")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Compare reachable_nodes / max_distance between modes (small graphs only)",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=DEFAULT_WORKER_S3_ENDPOINT,
        help="S3 endpoint for workers inside cluster",
    )
    parser.add_argument(
        "--validation-endpoint",
        default=DEFAULT_HOST_S3_ENDPOINT,
        help="S3 endpoint accesible desde host para preflight",
    )
    parser.add_argument("--bucket", default="test-bucket")
    parser.add_argument("--key-prefix", default="graphs")

    args = parser.parse_args()

    graph_file = f"large_sssp_{args.nodes}.txt"

    standalone_output = None
    sa_time = None
    if not args.skip_standalone:
        print("Running standalone SSSP...")
        standalone_output = benchmark_standalone(
            graph_file, args.nodes, args.source, args.max_iterations
        )
        if standalone_output is not None:
            sa_time = standalone_output.get("execution_time_ms", 0)
            print(f"SSSP Standalone Processing Time (Execution): {sa_time} ms")
            print(f"  reachable_nodes: {standalone_output.get('reachable_nodes')}")
            print(f"  max_distance:    {standalone_output.get('max_distance')}")
        else:
            print("SSSP Standalone Processing Time (Execution): FAILED")

    burst_results = None
    burst_host_time = None
    burst_warm_time = None
    algo_time = None
    phase_metrics = None
    validation_performed = False
    validation_passed = None
    validation_skipped_reason = None
    validation_mode = None
    if not args.skip_burst:
        clean_burst_cluster()
        print("Running burst SSSP...")
        burst_host_time, burst_warm_time, algo_time, burst_results, phase_metrics = benchmark_burst(
            num_nodes=args.nodes,
            num_partitions=args.partitions,
            source_node=args.source,
            max_iterations=args.max_iterations,
            memory_mb=args.memory,
            granularity=args.granularity,
            ow_host=args.ow_host,
            ow_port=args.ow_port,
            s3_endpoint=args.s3_endpoint,
            validation_endpoint=args.validation_endpoint,
            bucket=args.bucket,
            key_prefix=args.key_prefix,
            backend=args.backend,
            chunk_size=args.chunk_size,
        )
        if burst_host_time is not None:
            print(f"SSSP Burst Time (Host Total / Cold): {burst_host_time} ms")
            if burst_warm_time is not None:
                print(f"SSSP Burst Time (Load + Execution / Warm): {burst_warm_time} ms")
            if algo_time is not None:
                print(f"SSSP Burst Processing Time (Distributed Span): {algo_time} ms")
                if burst_warm_time is not None:
                    overhead = burst_warm_time - algo_time
                    print(f"Warm Coordination Overhead: {overhead} ms ({(overhead / burst_warm_time) * 100:.1f}%)")
        else:
            print("SSSP Burst Time: FAILED")

    if sa_time is not None:
        if burst_warm_time is not None:
            standalone_total = standalone_output.get("total_time_ms", sa_time) if standalone_output else sa_time
            speedup_total = standalone_total / burst_warm_time
            print(f"\nWarm Total Speedup (Load + Execution): {speedup_total:.2f}x")
        if algo_time is not None:
            algo_speedup = sa_time / algo_time
            print(f"Processing Speedup (Algorithmic): {algo_speedup:.2f}x")
            if algo_speedup > 1.0:
                print("✓ Algorithmically, Burst is faster!")
            else:
                print("✗ Standalone is still faster (below crossover)")

    if args.validate:
        validation_mode = "exact" if args.nodes < 10_000_000 else "summary"
        title = "Exact Validation" if validation_mode == "exact" else "Summary Validation"
        print(f"\n=== Running {title} ===")
        validation_performed = True
        validation_passed = run_validation(
            standalone_output,
            burst_results or [],
            args.nodes,
            args.validation_endpoint,
            args.bucket,
            f"{args.key_prefix}/large-sssp-{args.nodes}",
        )
        if not validation_passed:
            print("\n✗ VALIDATION FAILED")
            sys.exit(1)
        print(f"\n✓ {title.upper()} PASSED")
    else:
        validation_skipped_reason = "validation not requested"

    summary = build_benchmark_summary(
        nodes=args.nodes,
        source_node=args.source,
        max_iterations=args.max_iterations,
        partitions=args.partitions,
        granularity=args.granularity,
        memory_mb=args.memory,
        standalone_output=standalone_output,
        burst_host_total_ms=burst_host_time,
        burst_warm_total_ms=burst_warm_time,
        burst_algo_ms=algo_time,
        validation_requested=args.validate,
        validation_performed=validation_performed,
        validation_passed=validation_passed,
        validation_skipped_reason=validation_skipped_reason,
        validation_mode=validation_mode,
        key_prefix=args.key_prefix,
        phase_metrics=phase_metrics,
        backend=args.backend,
        chunk_size=args.chunk_size,
    )
    print(f"{BENCHMARK_JSON_PREFIX}{json.dumps(summary, sort_keys=True)}")
