#!/usr/bin/env python3
"""Compare standalone BFS with the Burst implementation."""
import argparse
import json
import subprocess
import sys
import os
import boto3
from pathlib import Path

from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_client.time_helper import get_millis
from bfs_utils import generate_bfs_payload

HERE = Path(__file__).resolve().parent
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
        raise RuntimeError("failed to clean Burst cluster before running bfs")

STANDALONE_BINARY = str(HERE / "bfs-standalone" / "target" / "release" / "bfs-standalone")
RAYON_BINARY = str(HERE / "bfs-rayon" / "target" / "release" / "bfs-rayon")
MPI_BINARY = str(HERE / "bfs-mpi" / "target" / "release" / "bfs-mpi")


def _run_single_node_binary(cmd, label, timeout, env=None):
    """Shared launcher for standalone/rayon/mpi binaries. Returns parsed JSON or None."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=HERE, env=env,
        )
        if result.returncode != 0:
            print(f"Error running {label}: {result.stderr}", file=sys.stderr)
            return None
        return json.loads(result.stdout.strip())
    except subprocess.TimeoutExpired:
        print(f"Error: {label} timed out", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing {label} output: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return None


def _make_s3_client(endpoint: str):
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def download_burst_levels(bucket: str, key: str, endpoint: str) -> dict | None:
    try:
        s3 = _make_s3_client(endpoint)
        obj = s3.get_object(Bucket=bucket, Key=f"{key}/output/bfs_levels_final.json")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as exc:
        print(f"Validation: could not fetch burst BFS levels: {exc}", file=sys.stderr)
        return None


def benchmark_standalone(graph_file: str, num_nodes: int, source_node: int, max_levels: int, timeout: int = 600):
    """Run the standalone (single-thread CSR) BFS binary."""
    if not os.path.exists(STANDALONE_BINARY):
        print(
            f"Error: Binary not found at {STANDALONE_BINARY}\n"
            "Run: cd bfs-standalone && cargo build --release",
            file=sys.stderr,
        )
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = [STANDALONE_BINARY, graph_file, str(num_nodes), str(source_node), str(max_levels)]
    return _run_single_node_binary(cmd, "standalone", timeout)


def benchmark_rayon(graph_file: str, num_nodes: int, source_node: int, max_levels: int, threads=None, timeout: int = 600):
    """Run Rayon (multi-thread shared-memory CSR) BFS."""
    if not os.path.exists(RAYON_BINARY):
        print(
            f"Error: Binary not found at {RAYON_BINARY}\n"
            "Run: cd bfs-rayon && cargo build --release",
            file=sys.stderr,
        )
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = [RAYON_BINARY, graph_file, str(num_nodes), str(source_node), str(max_levels)]
    if threads is not None:
        cmd.append(str(threads))
    return _run_single_node_binary(cmd, "rayon", timeout)


def benchmark_mpi(graph_file: str, num_nodes: int, source_node: int, max_levels: int, ranks: int, hosts=None, timeout: int = 600):
    """Run MPI (distributed CSR via Allreduce-MIN) BFS.

    `hosts` is an optional comma-separated list (e.g. "compute6,compute7") passed
    to `mpirun -H`. If `None`, ranks are scheduled by the local MPI runtime.
    """
    if not os.path.exists(MPI_BINARY):
        print(
            f"Error: Binary not found at {MPI_BINARY}\n"
            "Run: cd bfs-mpi && cargo build --release",
            file=sys.stderr,
        )
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = ["mpirun", "-np", str(ranks)]
    if hosts:
        cmd += ["-H", hosts]
    cmd += [MPI_BINARY, graph_file, str(num_nodes), str(source_node), str(max_levels)]
    return _run_single_node_binary(cmd, "mpi", timeout)


def benchmark_burst(
    num_nodes: int,
    num_partitions: int,
    source_node: int,
    max_levels: int,
    memory_mb: int,
    granularity: int = 1,
    ow_host: str = "localhost",
    ow_port: int = 31001,
    s3_endpoint: str = "http://minio-service.default.svc.cluster.local:9000",
    bucket: str = "test-bucket",
    key_prefix: str = "graphs",
    backend: str = "redis-list",
    chunk_size: int = 1024,
    register_action=True,
):
    """Run the distributed BFS burst action and return phase-aware metrics."""
    s3_prefix = f"{key_prefix}/large-bfs-{num_nodes}"

    params = generate_bfs_payload(
        endpoint=s3_endpoint,
        partitions=num_partitions,
        num_nodes=num_nodes,
        bucket=bucket,
        key=s3_prefix,
        source_node=source_node,
        max_levels=max_levels,
        granularity=granularity,
    )

    executor = OpenwhiskExecutor(ow_host, ow_port, debug=True)

    try:
        host_submit = get_millis()
        dt = executor.burst(
            "bfs",
            params,
            file="./bfs.zip",
            memory=memory_mb,
            custom_image="burstcomputing/runtime-rust-burst:latest",
            debug_mode=True,
            granularity=granularity,
            join=False,
            backend=backend,
            chunk_size=chunk_size,
            is_zip=True,
            timeout=1800000,
            register_action=register_action,
        )
        finished = get_millis()

        results = dt.get_results()
        if not results:
            print("Error: No results from burst BFS", file=sys.stderr)
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
        print(f"Error running burst BFS: {e}", file=sys.stderr)
        return None, None, None, None, None


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
    """Compare the full BFS levels vector between standalone and burst."""
    if not standalone_output or not burst_results:
        print("Validation: missing data", file=sys.stderr)
        return False

    sa_visited = standalone_output.get("visited_nodes", -1)
    sa_max_lv = standalone_output.get("max_level", -1)
    sa_levels = standalone_output.get("levels")
    burst_payload = download_burst_levels(bucket, key, endpoint)
    if burst_payload and isinstance(sa_levels, list):
        burst_levels = burst_payload.get("levels")
        if not isinstance(burst_levels, list):
            print("Validation: burst levels payload is missing levels", file=sys.stderr)
            return False
        if len(sa_levels) != len(burst_levels):
            print(
                f"  ✗ levels length mismatch: standalone={len(sa_levels)}, burst={len(burst_levels)}",
                file=sys.stderr,
            )
            return False
        for idx, (sa_level, burst_level) in enumerate(zip(sa_levels, burst_levels)):
            if sa_level != burst_level:
                print(
                    f"  ✗ level mismatch at node {idx}: standalone={sa_level}, burst={burst_level}",
                    file=sys.stderr,
                )
                return False
        print(f"  ✓ Full levels vector matches for {len(sa_levels)} nodes")
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

    burst_visited = _extract_report_stat(burst_report, "Visited nodes:", int)
    burst_max_lv = _extract_report_stat(burst_report, "Max BFS level:", int)

    ok = True
    if burst_visited is not None and sa_visited != burst_visited:
        print(
            f"  ✗ visited_nodes mismatch: standalone={sa_visited}, burst={burst_visited}",
            file=sys.stderr,
        )
        ok = False
    if burst_max_lv is not None and sa_max_lv != burst_max_lv:
        print(
            f"  ✗ max_level mismatch: standalone={sa_max_lv}, burst={burst_max_lv}",
            file=sys.stderr,
        )
        ok = False

    if ok:
        print(
            f"  ✓ visited_nodes={sa_visited}, max_level={sa_max_lv} — MATCH"
        )
    return ok


def pick_winner(speedup: float | None) -> str | None:
    if speedup is None:
        return None
    return "Burst" if speedup > 1.0 else "Standalone"


def build_benchmark_summary(
    nodes: int,
    source_node: int,
    max_levels: int,
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
        algorithm="bfs",
        num_nodes=nodes,
        workers=workers,
        iterations=(phase_metrics or {}).get("iterations", 0) or 0,
    )

    return {
        "algorithm": "bfs",
        "dataset": {
            "nodes": nodes,
            "graph_file": f"large_bfs_{nodes}.txt",
            "s3_prefix": f"{key_prefix}/large-bfs-{nodes}",
        },
        "configuration": {
            "source_node": source_node,
            "max_levels": max_levels,
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
            "visited_nodes": standalone_output.get("visited_nodes") if standalone_output else None,
            "max_level": standalone_output.get("max_level") if standalone_output else None,
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
            "mode": "exact" if validation_performed else None,
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark BFS: Standalone vs Burst")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--partitions", type=int, default=8, help="S3 partitions for burst")
    parser.add_argument("--granularity", type=int, default=1, help="Workers per Burst pack")
    parser.add_argument("--max-levels", type=int, default=500, help="Maximum BFS depth")
    parser.add_argument("--source", type=int, default=0, help="BFS source node")
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
        help="Compare visited_nodes / max_level between modes (small graphs only)",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=DEFAULT_WORKER_S3_ENDPOINT,
        help="S3 endpoint for workers inside cluster",
    )
    parser.add_argument(
        "--validation-endpoint",
        default=DEFAULT_HOST_S3_ENDPOINT,
        help="Host-accessible S3 endpoint for validation reads",
    )
    parser.add_argument("--bucket", default="test-bucket")
    parser.add_argument("--key-prefix", default="graphs")
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help=(
            "Skip the pre-run `clean_burst_cluster` step. Use this for the warm "
            "repetitions of the cold/warm protocol: the first invocation of a "
            "(n, p, g, memory) cell runs WITHOUT this flag (cold start, fresh "
            "pods); subsequent invocations of the same cell add the flag so the "
            "OpenWhisk pool can serve the request from a warm container."
        ),
    )
    parser.add_argument(
        "--skip-action-update",
        action="store_true",
        help="Do not re-register the OpenWhisk action before invoking. Keeps warm containers valid (re-registration bumps the action revision and invalidates the warm pool).",
    )
    parser.add_argument("--iter", dest="iter_alias", type=int, default=None,
                        help="Alias for --max-levels (used by run_cost_sweep). Overrides --max-levels if given.")
    parser.add_argument("--run-rayon", action="store_true", help="Also run Rayon backend")
    parser.add_argument("--rayon-threads", type=int, default=None, help="Rayon thread count (default: rayon's own choice)")
    parser.add_argument("--run-mpi", action="store_true", help="Also run MPI backend")
    parser.add_argument("--mpi-ranks", type=int, default=None, help="MPI rank count")
    parser.add_argument("--mpi-hosts", type=str, default=None, help="mpirun -H hostlist (e.g. compute6,compute7)")

    args = parser.parse_args()
    if args.iter_alias is not None:
        args.max_levels = args.iter_alias

    graph_file = f"large_bfs_{args.nodes}.txt"

    standalone_output = None
    lpst_time = None
    if not args.skip_standalone:
        print("Running standalone BFS...")
        standalone_output = benchmark_standalone(
            graph_file, args.nodes, args.source, args.max_levels
        )
        if standalone_output is not None:
            lpst_time = standalone_output.get("execution_time_ms", 0)
            print(f"BFS Standalone Processing Time (Execution): {lpst_time} ms")
            print(f"  visited_nodes: {standalone_output.get('visited_nodes')}")
            print(f"  max_level:     {standalone_output.get('max_level')}")
        else:
            print("BFS Standalone Processing Time (Execution): FAILED")

    burst_results = None
    burst_host_time = None
    burst_warm_time = None
    algo_time = None
    phase_metrics = None
    validation_performed = False
    validation_passed = None
    validation_skipped_reason = None
    if not args.skip_burst:
        if args.skip_clean:
            print("Skipping clean_burst_cluster (warm repetition).")
        else:
            clean_burst_cluster()
        print("Running burst BFS...")
        burst_host_time, burst_warm_time, algo_time, burst_results, phase_metrics = benchmark_burst(
            num_nodes=args.nodes,
            num_partitions=args.partitions,
            source_node=args.source,
            max_levels=args.max_levels,
            memory_mb=args.memory,
            granularity=args.granularity,
            ow_host=args.ow_host,
            ow_port=args.ow_port,
            s3_endpoint=args.s3_endpoint,
            bucket=args.bucket,
            key_prefix=args.key_prefix,
            backend=args.backend,
            chunk_size=args.chunk_size,
            register_action=not args.skip_action_update,
        )
        if burst_host_time is not None:
            print(f"BFS Burst Time (Host Total / Cold): {burst_host_time} ms")
            if burst_warm_time is not None:
                print(f"BFS Burst Time (Load + Execution / Warm): {burst_warm_time} ms")
            if algo_time is not None:
                print(f"BFS Burst Processing Time (Distributed Span): {algo_time} ms")
                if burst_warm_time is not None:
                    overhead = burst_warm_time - algo_time
                    print(f"Warm Coordination Overhead: {overhead} ms ({(overhead / burst_warm_time) * 100:.1f}%)")
        else:
            print("BFS Burst Time: FAILED")

    rayon_output = None
    rayon_time = None
    if args.run_rayon:
        print(f"Running Rayon backend (threads={args.rayon_threads})...")
        rayon_output = benchmark_rayon(
            graph_file, args.nodes, args.source, args.max_levels, args.rayon_threads,
        )
        if rayon_output is not None:
            rayon_time = rayon_output.get("execution_time_ms")
            print(f"Rayon Execution Time: {rayon_time} ms (threads={rayon_output.get('threads')})")
        else:
            print("Rayon: FAILED")

    mpi_output = None
    mpi_time = None
    if args.run_mpi:
        ranks = args.mpi_ranks or args.partitions
        print(f"Running MPI backend (ranks={ranks}, hosts={args.mpi_hosts})...")
        mpi_output = benchmark_mpi(
            graph_file, args.nodes, args.source, args.max_levels, ranks, args.mpi_hosts,
        )
        if mpi_output is not None:
            mpi_time = mpi_output.get("execution_time_ms")
            print(f"MPI Execution Time: {mpi_time} ms (ranks={mpi_output.get('ranks')})")
        else:
            print("MPI: FAILED")

    if lpst_time is not None:
        if burst_warm_time is not None:
            standalone_total = standalone_output.get("total_time_ms", lpst_time) if standalone_output else lpst_time
            speedup_total = standalone_total / burst_warm_time
            print(f"\nWarm Total Speedup (Load + Execution): {speedup_total:.2f}x")
        if algo_time is not None:
            algo_speedup = lpst_time / algo_time
            print(f"Processing Speedup (Algorithmic): {algo_speedup:.2f}x")
            if algo_speedup > 1.0:
                print("✓ Algorithmically, Burst is faster!")
            else:
                print("✗ Standalone is still faster (below crossover)")

    if args.validate:
        print("\n=== Running Exact Validation ===")
        validation_performed = True
        validation_passed = run_validation(
            standalone_output,
            burst_results or [],
            args.nodes,
            args.validation_endpoint,
            args.bucket,
            f"{args.key_prefix}/large-bfs-{args.nodes}",
        )
        if not validation_passed:
            print("\n✗ VALIDATION FAILED")
            sys.exit(1)
        print("\n✓ VALIDATION PASSED")
    else:
        validation_skipped_reason = "validation not requested"

    summary = build_benchmark_summary(
        nodes=args.nodes,
        source_node=args.source,
        max_levels=args.max_levels,
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
        key_prefix=args.key_prefix,
        phase_metrics=phase_metrics,
        backend=args.backend,
        chunk_size=args.chunk_size,
    )
    print(f"{BENCHMARK_JSON_PREFIX}{json.dumps(summary, sort_keys=True)}")
