#!/usr/bin/env python3
"""
Benchmark Label Propagation: Standalone vs Burst versions
"""
import argparse
import json
import tempfile
import subprocess
import sys
import os
from pathlib import Path
import boto3
from botocore.config import Config
from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_client.time_helper import get_millis
from labelpropagation_utils import generate_payload

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(HERE) not in sys.path:
    sys.path.append(str(HERE))

from runtime_metrics import compute_phase_breakdown, estimate_logical_traffic_bytes

DEFAULT_WORKER_S3_ENDPOINT = os.environ.get("S3_WORKER_ENDPOINT", "http://minio-service.default.svc.cluster.local:9000")
DEFAULT_HOST_S3_ENDPOINT = os.environ.get("S3_HOST_ENDPOINT", "http://localhost:9000")
BENCHMARK_JSON_PREFIX = "BENCHMARK_RESULT_JSON:"
CLEAN_BURST_CLUSTER_SCRIPT = ROOT / "clean_burst_cluster.sh"


def delete_burst_output(bucket, key, endpoint):
    """Delete previous LP output object to avoid validating stale results."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    output_key = f"{key}/output/labels_final.json"
    try:
        s3_client.delete_object(Bucket=bucket, Key=output_key)
        print(f"Deleted previous burst output: s3://{bucket}/{output_key}")
    except Exception as exc:
        print(f"Warning: could not delete previous burst output {output_key}: {exc}", file=sys.stderr)


def clean_burst_cluster(namespace="openwhisk", release_name="owdev"):
    """Delete stale guest/prewarm OpenWhisk pods before a Burst run."""
    result = subprocess.run(
        ["bash", str(CLEAN_BURST_CLUSTER_SCRIPT), namespace, release_name],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError("failed to clean Burst cluster before running labelpropagation")


def _make_s3_client(endpoint):
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def lp_partitions_available(bucket, key_prefix, endpoint, partitions):
    try:
        s3 = _make_s3_client(endpoint)
        response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{key_prefix}/part-")
        keys = {
            item["Key"]
            for item in response.get("Contents", [])
            if item.get("Key", "").startswith(f"{key_prefix}/part-")
        }
        expected = {f"{key_prefix}/part-{part_id:05d}" for part_id in range(partitions)}
        if keys != expected:
            return False
        return True
    except Exception:
        return False


def ensure_input_data(num_nodes, partitions, bucket, endpoint, key_prefix, graph_file, density=20):
    s3_prefix = f"{key_prefix}/large-{num_nodes}"
    local_ready = os.path.exists(graph_file) and os.path.getsize(graph_file) > 0
    s3_ready = lp_partitions_available(bucket, s3_prefix, endpoint, partitions)
    if local_ready and s3_ready:
        return

    print("Preparing LP input data...")
    result = subprocess.run(
        [
            sys.executable,
            "setup_large_lp_data.py",
            "--nodes", str(num_nodes),
            "--partitions", str(partitions),
            "--bucket", bucket,
            "--endpoint", endpoint,
            "--density", str(density),
            "--prefix", s3_prefix,
            "--output", graph_file,
        ],
        capture_output=True,
        text=True,
        cwd=HERE,
        timeout=1800,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "failed to prepare LP input data\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

def _run_single_node_binary(cmd, label, timeout):
    """Shared launcher for standalone/rayon binaries. Returns parsed JSON or None."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=HERE,
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


def benchmark_standalone(graph_file, num_nodes, max_iter, timeout=600):
    """Run standalone (single-thread CSR) Label Propagation."""
    binary_path = HERE / "lpst" / "target" / "release" / "label-propagation"
    if not binary_path.exists():
        print(f"Error: Binary not found at {binary_path}", file=sys.stderr)
        print("Run: cd lpst && cargo build --release", file=sys.stderr)
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = [str(binary_path), graph_file, str(num_nodes), str(max_iter)]
    return _run_single_node_binary(cmd, "standalone", timeout)


def benchmark_rayon(graph_file, num_nodes, max_iter, threads=None, timeout=600):
    """Run Rayon (multi-thread shared-memory CSR) Label Propagation."""
    binary_path = HERE / "lp-rayon" / "target" / "release" / "lp-rayon"
    if not binary_path.exists():
        print(f"Error: Binary not found at {binary_path}", file=sys.stderr)
        print("Run: cd lp-rayon && cargo build --release", file=sys.stderr)
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = [str(binary_path), graph_file, str(num_nodes), str(max_iter)]
    if threads is not None:
        cmd.append(str(threads))
    return _run_single_node_binary(cmd, "rayon", timeout)


def benchmark_mpi(graph_file, num_nodes, max_iter, ranks, hosts=None, timeout=600):
    """Run MPI (distributed CSR via Allreduce-MIN) Label Propagation.

    `hosts` is an optional comma-separated list (e.g. "compute6,compute7") passed
    to `mpirun -H`. If `None`, ranks are scheduled by the local MPI runtime.
    """
    binary_path = HERE / "lp-mpi" / "target" / "release" / "lp-mpi"
    if not binary_path.exists():
        print(f"Error: Binary not found at {binary_path}", file=sys.stderr)
        print("Run: cd lp-mpi && cargo build --release", file=sys.stderr)
        return None
    if not os.path.exists(graph_file):
        print(f"Error: Graph file not found: {graph_file}", file=sys.stderr)
        return None
    cmd = ["mpirun", "-np", str(ranks)]
    if hosts:
        cmd += ["-H", hosts]
    cmd += [str(binary_path), graph_file, str(num_nodes), str(max_iter)]
    return _run_single_node_binary(cmd, "mpi", timeout)

def run_validation(standalone_output, graph_file, num_nodes, max_iter, bucket, key, endpoint):
    """Run validation comparing standalone vs burst results."""
    if standalone_output is None:
        standalone_output = benchmark_standalone(graph_file, num_nodes, max_iter)
        if standalone_output is None:
            print("Standalone failed during validation setup", file=sys.stderr)
            return False

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            prefix="lpst_output_",
            delete=False,
        ) as handle:
            json.dump(standalone_output, handle)
            temp_path = handle.name

        val_result = subprocess.run(
            [
                sys.executable, "validate_results.py",
                "--standalone", temp_path,
                "--graph", graph_file,
                "--bucket", bucket,
                "--key", key,
                "--endpoint", endpoint,
                "--num-nodes", str(num_nodes),
            ],
            capture_output=True,
            text=True,
        )

        print(val_result.stdout)
        if val_result.returncode != 0:
            print("VALIDATION FAILED!", file=sys.stderr)
            print(val_result.stderr, file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"Error running validation: {e}", file=sys.stderr)
        return False
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)

def benchmark_burst(
    num_nodes,
    num_partitions,
    max_iter,
    memory_mb,
    granularity=1,
    ow_host="localhost",
    ow_port=31001,
    s3_endpoint="http://minio-service.default.svc.cluster.local:9000",
    bucket="test-bucket",
    key_prefix="graphs",
    backend="redis-list",
    chunk_size=1024,
    ow_protocol="https",
):
    """Run burst Label Propagation and return (host_total_ms, warm_total_ms, span_ms)."""
    s3_prefix = f"{key_prefix}/large-{num_nodes}"
    burst_timeout_ms = int(os.environ.get("LP_BURST_TIMEOUT_MS", "1800000"))
    
    params = generate_payload(
        endpoint=s3_endpoint,
        partitions=num_partitions,
        num_nodes=num_nodes,
        bucket=bucket,
        key=s3_prefix,
        convergence_threshold=0,
        max_iterations=max_iter,
        granularity=granularity
    )
    
    executor = OpenwhiskExecutor(ow_host, ow_port, debug=True, protocol=ow_protocol)
    
    try:
        host_submit = get_millis()
        dt = executor.burst(
            "labelpropagation",
            params,
            file=str(HERE / "labelpropagation.zip"),
            memory=memory_mb,
            custom_image="burstcomputing/runtime-rust-burst:latest",
            debug_mode=True,
            granularity=granularity,
            join=False,
            backend=backend,
            chunk_size=chunk_size,
            is_zip=True,
            timeout=burst_timeout_ms
        )
        finished = get_millis()
        
        # Get results to ensure completion
        results = dt.get_results()
        if not results:
            print("Error: No results from burst execution", file=sys.stderr)
            return None, None, None, None

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
            phase_metrics,
        )
    except Exception as e:
        print(f"Error running burst: {e}", file=sys.stderr)
        return None, None, None, None


def pick_winner(speedup):
    if speedup is None:
        return None
    return "Burst" if speedup > 1.0 else "Standalone"


def build_benchmark_summary(
    nodes,
    iterations,
    partitions,
    granularity,
    memory_mb,
    standalone_output,
    burst_host_total_ms,
    burst_warm_total_ms,
    burst_algo_ms,
    validation_requested,
    validation_performed,
    validation_passed,
    validation_skipped_reason,
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
    if standalone_exec_ms is not None and burst_algo_ms not in (None, 0):
        algo_speedup = standalone_exec_ms / burst_algo_ms
    if standalone_total_ms is not None and burst_warm_total_ms not in (None, 0):
        warm_speedup = standalone_total_ms / burst_warm_total_ms
    if standalone_total_ms is not None and burst_host_total_ms not in (None, 0):
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
        algorithm="labelpropagation",
        num_nodes=nodes,
        workers=workers,
        iterations=(phase_metrics or {}).get("iterations", 0) or 0,
    )

    return {
        "algorithm": "labelpropagation",
        "dataset": {
            "nodes": nodes,
            "graph_file": f"large_{nodes}.txt",
        },
        "configuration": {
            "partitions": partitions,
            "granularity": granularity,
            "max_iter": iterations,
            "memory_mb": memory_mb,
            "backend": backend,
            "chunk_size": chunk_size,
        },
        "standalone": {
            "compute_only_ms": standalone_exec_ms,
            "execution_time_ms": standalone_exec_ms,
            "end_to_end_ms": standalone_total_ms,
            "total_time_ms": standalone_total_ms,
            "num_labeled": standalone_output.get("num_labeled") if standalone_output else None,
            "num_communities": standalone_output.get("num_communities") if standalone_output else None,
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
    parser = argparse.ArgumentParser(description="Benchmark LP: Standalone vs Burst")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--graph-file", type=str, default=None, help="Override local graph file path")
    parser.add_argument("--partitions", type=int, default=8, help="Number of partitions for burst")
    parser.add_argument("--granularity", type=int, default=1, help="Workers per Burst pack")
    parser.add_argument("--iter", type=int, default=10, help="Max iterations")
    parser.add_argument("--memory", type=int, default=512, help="Memory per worker (MB)")
    parser.add_argument("--ow-host", type=str, default="localhost", help="OpenWhisk host")
    parser.add_argument("--ow-port", type=int, default=31001, help="OpenWhisk port")
    parser.add_argument("--ow-protocol", type=str, default=os.environ.get("OW_PROTOCOL", "https"), choices=["http", "https"], help="OpenWhisk protocol")
    parser.add_argument("--ow-k8s-namespace", type=str, default=os.environ.get("OPENWHISK_K8S_NAMESPACE", "openwhisk"), help="Kubernetes namespace that hosts the OpenWhisk release")
    parser.add_argument("--ow-release-name", type=str, default=os.environ.get("OPENWHISK_RELEASE_NAME", "owdev"), help="Helm release name of the OpenWhisk deployment")
    parser.add_argument("--skip-standalone", action="store_true", help="Skip standalone benchmark")
    parser.add_argument("--skip-burst", action="store_true", help="Skip burst benchmark")
    parser.add_argument("--backend", default="redis-list", help="Burst communication backend")
    parser.add_argument("--chunk-size", type=int, default=1024, help="Burst middleware chunk size in KB")
    parser.add_argument("--validate", action="store_true", help="Validate burst results against standalone")
    parser.add_argument("--skip-input-ensure", action="store_true", help="Skip local/S3 input dataset preparation checks")
    parser.add_argument("--skip-output-delete", action="store_true", help="Skip deleting previous Burst output object before execution")
    parser.add_argument("--s3-endpoint", default=DEFAULT_WORKER_S3_ENDPOINT, help="S3 endpoint for workers inside cluster")
    parser.add_argument("--validation-endpoint", default=DEFAULT_HOST_S3_ENDPOINT, help="S3 endpoint for local validation script")
    parser.add_argument("--bucket", default="test-bucket", help="S3 bucket name")
    parser.add_argument("--key-prefix", default="graphs", help="S3 key prefix")
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
    parser.add_argument("--run-rayon", action="store_true", help="Also run Rayon backend")
    parser.add_argument("--rayon-threads", type=int, default=None, help="Rayon thread count (default: rayon's own choice)")
    parser.add_argument("--run-mpi", action="store_true", help="Also run MPI backend")
    parser.add_argument("--mpi-ranks", type=int, default=None, help="MPI rank count")
    parser.add_argument("--mpi-hosts", type=str, default=None, help="mpirun -H hostlist (e.g. compute6,compute7)")
    
    args = parser.parse_args()
    
    graph_file = args.graph_file or str(HERE / f"large_{args.nodes}.txt")
    if not args.skip_input_ensure:
        ensure_input_data(
            num_nodes=args.nodes,
            partitions=args.partitions,
            bucket=args.bucket,
            endpoint=args.validation_endpoint,
            key_prefix=args.key_prefix,
            graph_file=graph_file,
        )
    
    # Benchmark standalone
    standalone_output = None
    lpst_time = None
    if not args.skip_standalone:
        print(f"Running standalone version...")
        standalone_output = benchmark_standalone(graph_file, args.nodes, args.iter)
        if standalone_output is not None:
            lpst_time = standalone_output.get("execution_time_ms", 0)
            print(f"Standalone Processing Time (Execution): {lpst_time} ms")
        else:
            print("Standalone Processing Time (Execution): FAILED")
    
    # Benchmark burst
    burst_host_time = None
    burst_warm_time = None
    algo_time = None
    phase_metrics = None
    validation_performed = False
    validation_passed = None
    validation_skipped_reason = None
    if not args.skip_burst:
        burst_key_prefix = f"{args.key_prefix}/large-{args.nodes}"
        if args.skip_clean:
            print("Skipping clean_burst_cluster (warm repetition).")
        else:
            clean_burst_cluster(args.ow_k8s_namespace, args.ow_release_name)
        if not args.skip_output_delete:
            delete_burst_output(args.bucket, burst_key_prefix, args.validation_endpoint)
        print(f"Running burst version...")
        burst_host_time, burst_warm_time, algo_time, phase_metrics = benchmark_burst(
            args.nodes, 
            args.partitions, 
            args.iter, 
            args.memory,
            args.granularity,
            args.ow_host,
            args.ow_port,
            args.s3_endpoint,
            args.bucket,
            args.key_prefix,
            args.backend,
            args.chunk_size,
            args.ow_protocol,
        )
        if burst_host_time is not None:
            print(f"Burst Time (Host Total / Cold): {burst_host_time} ms")
            if burst_warm_time is not None:
                print(f"Burst Time (Load + Execution / Warm): {burst_warm_time} ms")
            if algo_time:
                print(f"Burst Processing Time (Distributed Span): {algo_time} ms")
                if burst_warm_time is not None:
                    overhead = burst_warm_time - algo_time
                    print(f"Warm Coordination Overhead: {overhead} ms ({(overhead/burst_warm_time)*100:.1f}%)")
        else:
            print("Burst Time: FAILED")
    
    # Optional extra single-node backends (cost_sweep phase).
    rayon_output = None
    rayon_time = None
    if args.run_rayon:
        print(f"Running Rayon backend (threads={args.rayon_threads})...")
        rayon_output = benchmark_rayon(graph_file, args.nodes, args.iter, args.rayon_threads)
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
        mpi_output = benchmark_mpi(graph_file, args.nodes, args.iter, ranks, args.mpi_hosts)
        if mpi_output is not None:
            mpi_time = mpi_output.get("execution_time_ms")
            print(f"MPI Execution Time: {mpi_time} ms (ranks={mpi_output.get('ranks')})")
        else:
            print("MPI: FAILED")

    # Calculate speedup
    if lpst_time:
        if burst_warm_time:
            standalone_total = standalone_output.get("total_time_ms", lpst_time) if standalone_output else lpst_time
            speedup = standalone_total / burst_warm_time
            print(f"\nWarm Total Speedup (Load + Execution): {speedup:.2f}x")
        
        if algo_time:
            algo_speedup = lpst_time / algo_time
            print(f"Processing Speedup (Algorithmic): {algo_speedup:.2f}x")
            
            if algo_speedup > 1.0:
                print("✓ Algorithmically, Burst is faster!")
            else:
                print("✗ Even algorithmically, Standalone is faster")
    
    # Validation
    if args.validate:
        key_prefix = f"{args.key_prefix}/large-{args.nodes}"
        if args.nodes >= 10_000_000:
            validation_skipped_reason = "exact validation output is disabled for LP datasets with >= 10,000,000 nodes"
            validation_passed = None
            print("\n=== Skipping Exact Validation ===")
            print(validation_skipped_reason)
        elif burst_host_time is None:
            validation_skipped_reason = "burst execution failed before validation"
            validation_passed = False
            print("\n=== Skipping Exact Validation ===")
            print("Burst execution failed before validation; stale S3 output was deleted.")
            sys.exit(1)
        else:
            print("\n=== Running Exact Validation ===")
            validation_performed = True
            validation_passed = run_validation(
                standalone_output,
                graph_file,
                args.nodes,
                args.iter,
                args.bucket,
                key_prefix,
                args.validation_endpoint,
            )
            if not validation_passed:
                print("\n✗ VALIDATION FAILED - Results do not match!")
                sys.exit(1)
            print("\n✓ VALIDATION PASSED - Results match!")
    else:
        validation_skipped_reason = "validation not requested"

    summary = build_benchmark_summary(
        nodes=args.nodes,
        iterations=args.iter,
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
        phase_metrics=phase_metrics,
        backend=args.backend,
        chunk_size=args.chunk_size,
    )
    print(f"{BENCHMARK_JSON_PREFIX}{json.dumps(summary, sort_keys=True)}")
