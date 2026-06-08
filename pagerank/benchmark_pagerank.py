#!/usr/bin/env python3
"""Compare standalone PageRank with the Burst implementation. Mirror of
benchmark_sssp.py — same flag surface so run_cloudlab_campaign.py can drive
PageRank cells via the same Burst orchestrator path."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_client.time_helper import get_millis
from pagerank_utils import generate_pagerank_payload

HERE = Path(__file__).resolve().parent
ROOT = Path(__file__).resolve().parents[1]
LP_DIR = ROOT / "labelpropagation"
if str(LP_DIR) not in sys.path:
    sys.path.append(str(LP_DIR))

from runtime_metrics import compute_phase_breakdown, estimate_logical_traffic_bytes

DEFAULT_WORKER_S3_ENDPOINT = os.environ.get(
    "S3_WORKER_ENDPOINT", "http://minio-service.default.svc.cluster.local:9000"
)
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
        raise RuntimeError("failed to clean Burst cluster before running pagerank")


STANDALONE_BINARY = str(HERE / "pagerank-standalone" / "target" / "release" / "pagerank-standalone")
RAYON_BINARY = str(HERE / "pagerank-rayon" / "target" / "release" / "pagerank-rayon")
MPI_BINARY = str(HERE / "pagerank-mpi" / "target" / "release" / "pagerank-mpi")


def _run_single_node_binary(cmd, label, timeout, env=None):
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=HERE, env=env,
        )
        if result.returncode != 0:
            print(f"Error running {label}: {result.stderr}", file=sys.stderr)
            return None
        # Some binaries (rayon, mpi) emit progress lines + a single JSON line. Pick the last well-formed JSON.
        last_json = None
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    last_json = json.loads(line)
                except json.JSONDecodeError:
                    pass
        if last_json is None:
            print(f"Error: {label} produced no JSON output", file=sys.stderr)
        return last_json
    except subprocess.TimeoutExpired:
        print(f"Error: {label} timed out", file=sys.stderr)
        return None


def benchmark_standalone(graph_file: str, num_nodes: int, max_iter: int, timeout: int = 600):
    if not os.path.exists(STANDALONE_BINARY):
        print(f"Error: Binary not found at {STANDALONE_BINARY}", file=sys.stderr)
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = [STANDALONE_BINARY, graph_file, str(num_nodes), str(max_iter)]
    return _run_single_node_binary(cmd, "standalone", timeout)


def benchmark_rayon(graph_file: str, num_nodes: int, max_iter: int, threads=None, timeout: int = 600):
    if not os.path.exists(RAYON_BINARY):
        print(f"Error: Binary not found at {RAYON_BINARY}", file=sys.stderr)
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = [RAYON_BINARY, graph_file, str(num_nodes), str(max_iter)]
    if threads is not None:
        cmd.append(str(threads))
    return _run_single_node_binary(cmd, "rayon", timeout)


def benchmark_mpi(graph_file: str, num_nodes: int, max_iter: int, ranks: int, hosts=None, timeout: int = 600):
    if not os.path.exists(MPI_BINARY):
        print(f"Error: Binary not found at {MPI_BINARY}", file=sys.stderr)
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = ["mpirun", "-np", str(ranks)]
    if hosts:
        cmd += ["-H", hosts]
    cmd += [MPI_BINARY, graph_file, str(num_nodes), str(max_iter)]
    return _run_single_node_binary(cmd, "mpi", timeout)


def _is_loopback_endpoint(endpoint: str) -> bool:
    raw = (endpoint or "").strip().lower()
    if "://" not in raw:
        raw = f"http://{raw}"
    host_port = raw.split("://", 1)[1].split("/", 1)[0]
    host = host_port.split("@")[-1].split(":", 1)[0].strip("[]")
    return host in {"localhost", "127.0.0.1", "::1"}


def preflight_pagerank_input(
    num_nodes: int, num_partitions: int, validation_endpoint: str, bucket: str, s3_prefix: str,
):
    endpoint = validation_endpoint if validation_endpoint.startswith("http") else f"http://{validation_endpoint}"
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
        generation_cmd = (
            f"python3 setup_large_pagerank_data.py --nodes {num_nodes} "
            f"--partitions {num_partitions} --bucket {bucket} --endpoint {endpoint}"
        )
        message = (
            "Error: Missing or empty PageRank burst input in S3.\n"
            f"Checked endpoint={endpoint}, bucket={bucket}.\n"
            f"Expected partition keys under: {s3_prefix}/\n"
            f"Missing: {missing[:10]}"
            + (f" ... ({len(missing)} total)" if len(missing) > 10 else "")
            + f"\nGenerate data first with:\n  {generation_cmd}"
        )
        return False, message
    except (BotoCoreError, ClientError) as exc:
        return False, (
            "Error: S3 preflight failed before burst invocation.\n"
            f"Endpoint={endpoint}, bucket={bucket}, partition prefix={s3_prefix}.\n"
            f"S3 error: {exc}"
        )


def benchmark_burst(
    num_nodes: int,
    num_partitions: int,
    max_iter: int,
    memory_mb: int,
    granularity: int = 1,
    ow_host: str = "localhost",
    ow_port: int = 31001,
    ow_protocol: str = "https",
    s3_endpoint: str = DEFAULT_WORKER_S3_ENDPOINT,
    validation_endpoint: str = DEFAULT_HOST_S3_ENDPOINT,
    bucket: str = "test-bucket",
    key_prefix: str = "graphs",
    backend: str = "redis-list",
    chunk_size: int = 1024,
):
    if _is_loopback_endpoint(s3_endpoint):
        print(
            f"Error: Invalid worker S3 endpoint: {s3_endpoint}. "
            "Use --s3-endpoint with the internal cluster endpoint.",
            file=sys.stderr,
        )
        return None, None, None, None, None

    s3_prefix = f"{key_prefix}/large-pagerank-{num_nodes}"
    ok, preflight_error = preflight_pagerank_input(
        num_nodes=num_nodes,
        num_partitions=num_partitions,
        validation_endpoint=validation_endpoint,
        bucket=bucket,
        s3_prefix=s3_prefix,
    )
    if not ok:
        print(preflight_error, file=sys.stderr)
        return None, None, None, None, None

    params = generate_pagerank_payload(
        endpoint=s3_endpoint,
        partitions=num_partitions,
        num_nodes=num_nodes,
        bucket=bucket,
        key=s3_prefix,
        max_iterations=max_iter,
        granularity=granularity,
    )

    executor = OpenwhiskExecutor(ow_host, ow_port, debug=True, protocol=ow_protocol)
    try:
        host_submit = get_millis()
        dt = executor.burst(
            "pagerank",
            params,
            file="./pagerank.zip",
            memory=memory_mb,
            custom_image="burstcomputing/runtime-rust-burst:latest",
            debug_mode=True,
            granularity=granularity,
            join=False,
            backend=backend,
            chunk_size=chunk_size,
            is_zip=True,
            timeout=1800000,
        )
        finished = get_millis()
        results = dt.get_results()
        if not results:
            print("Error: No results from burst PageRank", file=sys.stderr)
            return None, None, None, None, None

        for r in results:
            worker_data = r[0] if isinstance(r, list) and r else r
            if isinstance(worker_data, dict) and worker_data.get("results"):
                print(worker_data["results"])

        phase_metrics = compute_phase_breakdown(
            results, host_submit_ms=host_submit, host_finished_ms=finished,
        )
        return (
            finished - host_submit,
            phase_metrics.get("warm_total_ms"),
            phase_metrics.get("span_ms"),
            results,
            phase_metrics,
        )
    except Exception as e:
        print(f"Error running burst PageRank: {e}", file=sys.stderr)
        return None, None, None, None, None


def pick_winner(speedup):
    if speedup is None:
        return None
    return "Burst" if speedup > 1.0 else "Standalone"


def build_benchmark_summary(
    nodes,
    max_iter,
    partitions,
    granularity,
    memory_mb,
    standalone_output,
    burst_host_total_ms,
    burst_warm_total_ms,
    burst_algo_ms,
    key_prefix,
    phase_metrics,
    backend,
    chunk_size,
):
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
    traffic = estimate_logical_traffic_bytes(
        algorithm="pagerank",
        num_nodes=nodes,
        workers=partitions,
        iterations=(phase_metrics or {}).get("iterations", 0) or 0,
    )

    return {
        "algorithm": "pagerank",
        "dataset": {
            "nodes": nodes,
            "graph_file": f"large_pagerank_{nodes}.txt",
            "s3_prefix": f"{key_prefix}/large-pagerank-{nodes}",
        },
        "configuration": {
            "max_iterations": max_iter,
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
            "iterations": standalone_output.get("iterations") if standalone_output else None,
            "max_rank": standalone_output.get("max_rank") if standalone_output else None,
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
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark PageRank: Standalone vs Burst")
    parser.add_argument("--nodes", type=int, required=True)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--granularity", type=int, default=1)
    parser.add_argument("--max-iterations", type=int, default=100)
    parser.add_argument("--memory", type=int, default=512)
    parser.add_argument("--ow-host", type=str, default="localhost")
    parser.add_argument("--ow-port", type=int, default=31001)
    parser.add_argument("--ow-protocol", type=str, default="https", help="OW endpoint scheme (http or https)")
    parser.add_argument("--skip-standalone", action="store_true")
    parser.add_argument("--skip-burst", action="store_true")
    parser.add_argument("--backend", default="redis-list")
    parser.add_argument("--chunk-size", type=int, default=1024)
    parser.add_argument("--s3-endpoint", default=DEFAULT_WORKER_S3_ENDPOINT)
    parser.add_argument("--validation-endpoint", default=DEFAULT_HOST_S3_ENDPOINT)
    parser.add_argument("--bucket", default="test-bucket")
    parser.add_argument("--key-prefix", default="graphs")
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help="Skip pre-run clean_burst_cluster (use for warm repetitions).",
    )
    parser.add_argument("--iter", dest="iter_alias", type=int, default=None,
                        help="Alias for --max-iterations (used by run_cost_sweep).")
    parser.add_argument("--run-rayon", action="store_true")
    parser.add_argument("--rayon-threads", type=int, default=None)
    parser.add_argument("--run-mpi", action="store_true")
    parser.add_argument("--mpi-ranks", type=int, default=None)
    parser.add_argument("--mpi-hosts", type=str, default=None)

    args = parser.parse_args()
    if args.iter_alias is not None:
        args.max_iterations = args.iter_alias

    graph_file = f"large_pagerank_{args.nodes}.txt"

    standalone_output = None
    sa_time = None
    if not args.skip_standalone:
        print("Running standalone PageRank...")
        standalone_output = benchmark_standalone(graph_file, args.nodes, args.max_iterations)
        if standalone_output is not None:
            sa_time = standalone_output.get("execution_time_ms", 0)
            print(f"PageRank Standalone Time: {sa_time} ms")
            print(f"  iterations: {standalone_output.get('iterations')}")
            print(f"  max_rank:   {standalone_output.get('max_rank')}")
        else:
            print("PageRank Standalone: FAILED")

    burst_results = None
    burst_host_time = None
    burst_warm_time = None
    algo_time = None
    phase_metrics = None
    if not args.skip_burst:
        if args.skip_clean:
            print("Skipping clean_burst_cluster (warm repetition).")
        else:
            clean_burst_cluster()
        print("Running burst PageRank...")
        burst_host_time, burst_warm_time, algo_time, burst_results, phase_metrics = benchmark_burst(
            num_nodes=args.nodes,
            num_partitions=args.partitions,
            max_iter=args.max_iterations,
            memory_mb=args.memory,
            granularity=args.granularity,
            ow_host=args.ow_host,
            ow_port=args.ow_port,
            ow_protocol=args.ow_protocol,
            s3_endpoint=args.s3_endpoint,
            validation_endpoint=args.validation_endpoint,
            bucket=args.bucket,
            key_prefix=args.key_prefix,
            backend=args.backend,
            chunk_size=args.chunk_size,
        )
        if burst_host_time is not None:
            print(f"PageRank Burst Time (Host Total / Cold): {burst_host_time} ms")
            if burst_warm_time is not None:
                print(f"PageRank Burst Time (Warm): {burst_warm_time} ms")
            if algo_time is not None:
                print(f"PageRank Burst Span: {algo_time} ms")
        else:
            print("PageRank Burst: FAILED")

    if args.run_rayon:
        print(f"Running Rayon backend (threads={args.rayon_threads})...")
        rayon_output = benchmark_rayon(
            graph_file, args.nodes, args.max_iterations, args.rayon_threads,
        )
        if rayon_output is not None:
            print(f"Rayon Time: {rayon_output.get('execution_time_ms')} ms")
        else:
            print("Rayon: FAILED")

    if args.run_mpi:
        ranks = args.mpi_ranks or args.partitions
        print(f"Running MPI backend (ranks={ranks}, hosts={args.mpi_hosts})...")
        mpi_output = benchmark_mpi(
            graph_file, args.nodes, args.max_iterations, ranks, args.mpi_hosts,
        )
        if mpi_output is not None:
            print(f"MPI Time: {mpi_output.get('execution_time_ms')} ms")
        else:
            print("MPI: FAILED")

    summary = build_benchmark_summary(
        nodes=args.nodes,
        max_iter=args.max_iterations,
        partitions=args.partitions,
        granularity=args.granularity,
        memory_mb=args.memory,
        standalone_output=standalone_output,
        burst_host_total_ms=burst_host_time,
        burst_warm_total_ms=burst_warm_time,
        burst_algo_ms=algo_time,
        key_prefix=args.key_prefix,
        phase_metrics=phase_metrics,
        backend=args.backend,
        chunk_size=args.chunk_size,
    )
    print(f"{BENCHMARK_JSON_PREFIX}{json.dumps(summary, sort_keys=True)}")
