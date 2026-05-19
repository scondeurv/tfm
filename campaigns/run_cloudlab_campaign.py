#!/usr/bin/env python3
"""Generalized CloudLab campaign runner for multiple graph algorithms.

Supports LP, BFS, SSSP, WCC using the same phase structure:
  preflight → characterization → config_sweep → chunk_probe → size_sweep → report

Algorithm-specific behavior is encapsulated in AlgorithmConfig.
Reuses infrastructure from run_cloudlab_lp_campaign.py.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Import shared infrastructure from LP campaign runner
from run_cloudlab_lp_campaign import (
    ROOT,
    BENCHMARK_PREFIX,
    REMOTE_DEFAULT_SRC_ROOT,
    utc_stamp,
    valid_granularities,
    characterization_granularities,
    write_json,
    read_json,
    run_command,
    ssh_command,
    scp_to_remote,
    scp_from_remote,
    ssh_python,
    capture_resource_snapshot,
    shell_quote_env,
    parse_prefixed_json,
    parse_json_lines,
    has_openwhisk_transport_failure,
    split_workers,
    spark_memory_to_k8s,
    _SPARK_MEMORY_RE,
    remote_delete_prefix,
    remote_lp_partitions_available,
    summarize_rows,
    failed_run_record,
    summarize_runtime_probes,
    pick_best_config,
    parse_burst_config_matrix,
    ensure_remote_openwhisk_api,
    run_runtime_probe,
    mean_std,
)

LP_DIR = ROOT / "labelpropagation"
BFS_DIR = ROOT / "bfs"
SSSP_DIR = ROOT / "sssp"
WCC_DIR = ROOT / "wcc"
EXPERIMENT_ROOT = ROOT / "experiment_data" / "cloudlab_campaigns"


@dataclasses.dataclass
class AlgorithmConfig:
    name: str                       # "lp", "bfs", "sssp", "wcc"
    display_name: str               # "Label Propagation", "BFS", ...
    workdir: Path                   # LP_DIR, BFS_DIR, ...
    benchmark_script: str           # "benchmark_lp.py", "benchmark_bfs.py"
    spark_smoke_script: str         # "run_cloudlab_smoke_lp_spark.sh"
    data_generator: str             # "setup_large_lp_data.py"
    graph_file_pattern: str         # "large_lp_{nodes}.txt"
    s3_prefix_infix: str            # "lp", "bfs", "sssp", "wcc"
    s3_dataset_basename: str        # "large", "large-bfs", "large-sssp", "wcc" — must match {key_prefix}/{basename}-{nodes} in benchmark_*.py
    burst_action_zip: str           # "labelpropagation.zip", "bfs.zip", ...
    extra_data_gen_args: list[str] = dataclasses.field(default_factory=list)


ALGORITHMS: dict[str, AlgorithmConfig] = {
    "lp": AlgorithmConfig(
        name="lp",
        display_name="Label Propagation",
        workdir=LP_DIR,
        benchmark_script="benchmark_lp.py",
        spark_smoke_script="run_cloudlab_smoke_lp_spark.sh",
        data_generator="setup_large_lp_data.py",
        graph_file_pattern="large_lp_{nodes}.txt",
        s3_prefix_infix="lp",
        s3_dataset_basename="large",
        burst_action_zip="labelpropagation.zip",
    ),
    "bfs": AlgorithmConfig(
        name="bfs",
        display_name="BFS",
        workdir=BFS_DIR,
        benchmark_script="benchmark_bfs.py",
        spark_smoke_script="run_cloudlab_smoke_bfs_spark.sh",
        data_generator="setup_large_bfs_data.py",
        graph_file_pattern="large_bfs_{nodes}.txt",
        s3_prefix_infix="bfs",
        s3_dataset_basename="large-bfs",
        burst_action_zip="bfs.zip",
    ),
    "sssp": AlgorithmConfig(
        name="sssp",
        display_name="SSSP",
        workdir=SSSP_DIR,
        benchmark_script="benchmark_sssp.py",
        spark_smoke_script="run_cloudlab_smoke_sssp_spark.sh",
        data_generator="setup_large_sssp_data.py",
        graph_file_pattern="large_sssp_{nodes}.txt",
        s3_prefix_infix="sssp",
        s3_dataset_basename="large-sssp",
        burst_action_zip="sssp.zip",
    ),
    "wcc": AlgorithmConfig(
        name="wcc",
        display_name="WCC",
        workdir=WCC_DIR,
        benchmark_script="benchmark_uf.py",
        spark_smoke_script="run_cloudlab_smoke_wcc_spark.sh",
        data_generator="setup_large_uf_data.py",
        graph_file_pattern="wcc_graph_{nodes}.tsv",
        s3_prefix_infix="wcc",
        s3_dataset_basename="wcc",
        burst_action_zip="unionfind.zip",
        extra_data_gen_args=["--edges-per-node", "5", "--components", "10", "--format", "tsv", "--no-s3"],
    ),
}


def algo_repo_python(algo: AlgorithmConfig) -> str:
    candidate = algo.workdir / ".venv" / "bin" / "python"
    return str(candidate if candidate.exists() else Path(sys.executable))


def remote_algo_dir(args: argparse.Namespace, algo: AlgorithmConfig) -> str:
    return f"{args.cloudlab_src_root}/{algo.workdir.name}"


def algo_burst_key_prefix(args: argparse.Namespace, algo: AlgorithmConfig) -> str:
    return f"cloudlab/campaigns/{args.campaign_root.name}/burst/{algo.name}"


def algo_spark_key_prefix(args: argparse.Namespace, algo: AlgorithmConfig) -> str:
    return f"cloudlab/campaigns/{args.campaign_root.name}/spark/{algo.name}"


# ---------------------------------------------------------------------------
# Graph data management
# ---------------------------------------------------------------------------

def ensure_local_graph_file(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    *,
    nodes: int,
    partitions: int,
) -> Path:
    dataset_dir = campaign_root / "datasets"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    graph_file = dataset_dir / algo.graph_file_pattern.format(nodes=nodes)
    burst_prefix = f"{algo_burst_key_prefix(args, algo)}/{algo.s3_dataset_basename}-{nodes}"
    if not graph_file.exists() or graph_file.stat().st_size <= 0:
        cmd = [
            algo_repo_python(algo),
            algo.data_generator,
            "--nodes", str(nodes),
            "--partitions", str(partitions),
            "--output", str(graph_file.resolve()),
        ]
        if algo.name != "wcc":
            cmd.extend(["--density", "10", "--no-s3"])
        else:
            cmd.extend(algo.extra_data_gen_args)
        completed = run_command(cmd, cwd=algo.workdir, timeout=1800)
        if completed.returncode != 0:
            raise RuntimeError(
                f"failed to generate {algo.name} dataset\n"
                f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
            )
    if not remote_lp_partitions_available(args, args.bucket, burst_prefix, partitions):
        upload_partitions_via_proxy(args, algo, campaign_root, graph_file, args.bucket, burst_prefix, partitions)
    return graph_file


def upload_partitions_via_proxy(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    graph_file: Path,
    bucket: str,
    prefix: str,
    partitions: int,
) -> None:
    remote_base = f"/tmp/{campaign_root.name}/datasets"
    remote_graph = f"{remote_base}/{graph_file.name}"
    mkdir_completed = ssh_command(args, f"mkdir -p {shlex.quote(remote_base)}")
    if mkdir_completed.returncode != 0:
        raise RuntimeError(f"failed to create remote dataset staging dir\nSTDERR:\n{mkdir_completed.stderr}")
    upload_completed = scp_to_remote(args, graph_file, remote_graph)
    if upload_completed.returncode != 0:
        raise RuntimeError(f"failed to upload graph file to CloudLab\nSTDERR:\n{upload_completed.stderr}")

    # Partition by source vertex modulo partition count (same scheme for all algorithms)
    if algo.name == "wcc":
        # WCC: undirected TSV, split on first column
        split_col = 0
    else:
        # LP/BFS/SSSP: directed TSV, split on source (col 0)
        split_col = 0

    script = f"""
import json
import shutil
from pathlib import Path

import boto3
from botocore.config import Config

bucket = {json.dumps(bucket)}
prefix = {json.dumps(prefix)}
partitions = {partitions}
graph_path = Path({json.dumps(remote_graph)})
part_dir = graph_path.parent / (graph_path.stem + "-parts")
part_dir.mkdir(parents=True, exist_ok=True)
handles = []
try:
    for part_id in range(partitions):
        handles.append((part_id, (part_dir / f"part-{{part_id:05d}}").open("w", encoding="utf-8")))
    with graph_path.open("r", encoding="utf-8") as source:
        for line in source:
            src = int(line.split("\\t", 1)[0])
            handles[src % partitions][1].write(line)
    for _, handle in handles:
        handle.close()
    s3 = boto3.client(
        "s3",
        endpoint_url={json.dumps(args.host_s3_endpoint)},
        aws_access_key_id={json.dumps(os.environ["AWS_ACCESS_KEY_ID"])},
        aws_secret_access_key={json.dumps(os.environ["AWS_SECRET_ACCESS_KEY"])},
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        for item in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=item["Key"])
    uploaded = []
    for part_id in range(partitions):
        local_part = part_dir / f"part-{{part_id:05d}}"
        if local_part.stat().st_size <= 0:
            raise RuntimeError(f"partition {{part_id}} is empty")
        key = f"{{prefix}}/part-{{part_id:05d}}"
        s3.upload_file(str(local_part), bucket, key)
        uploaded.append(key)
    print(json.dumps({{"uploaded": len(uploaded), "bucket": bucket, "prefix": prefix}}))
finally:
    for _, handle in handles:
        try:
            handle.close()
        except Exception:
            pass
    shutil.rmtree(part_dir, ignore_errors=True)
"""
    completed = ssh_python(
        args,
        script,
        timeout=7200,
        log_path=campaign_root / "logs" / "dataset_staging" / f"upload_{algo.name}_{graph_file.stem}.log",
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"failed to upload {algo.name} partitions\n"
            f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )


# ---------------------------------------------------------------------------
# Burst benchmark invocation (algorithm-specific CLI args)
# ---------------------------------------------------------------------------

def _burst_args_lp(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file) -> list[str]:
    load_prefix = f"{algo_burst_key_prefix(args, algo)}/large-{nodes}"
    return [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--iter", str(args.max_iter),
        "--memory", str(memory_mb),
        "--ow-host", "127.0.0.1",
        "--ow-port", str(args.ow_port),
        "--ow-protocol", shlex.quote(args.ow_protocol),
        "--ow-k8s-namespace", shlex.quote(args.ow_namespace),
        "--ow-release-name", shlex.quote(args.ow_release_name),
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--skip-standalone",
        "--skip-input-ensure",
        "--skip-output-delete",
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]


def _burst_args_bfs(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file) -> list[str]:
    return [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--source", str(args.source_node),
        "--max-levels", str(args.max_levels),
        "--memory", str(memory_mb),
        "--ow-host", "127.0.0.1",
        "--ow-port", str(args.ow_port),
        "--skip-standalone",
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]


def _burst_args_sssp(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file) -> list[str]:
    return [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--source", str(args.source_node),
        "--max-iterations", str(args.max_iter),
        "--memory", str(memory_mb),
        "--ow-host", "127.0.0.1",
        "--ow-port", str(args.ow_port),
        "--skip-standalone",
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]


def _burst_args_wcc(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file) -> list[str]:
    dataset_prefix = f"{algo_burst_key_prefix(args, algo)}"
    return [
        "python3", algo.benchmark_script,
        "--ow-host", "127.0.0.1",
        "--ow-port", str(args.ow_port),
        "--runtime-memory", str(memory_mb),
        "--backend", shlex.quote(args.backend),
        "--granularity", str(granularity),
        "--chunk-size", str(args.chunk_size_kb),
        "--wcc-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--local-endpoint", shlex.quote(args.host_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--partitions", str(partitions),
        "--input-format", "tsv",
        "--sizes", str(nodes),
        "--skip-standalone",
    ]


BURST_ARG_BUILDERS: dict[str, Callable] = {
    "lp": _burst_args_lp,
    "bfs": _burst_args_bfs,
    "sssp": _burst_args_sssp,
    "wcc": _burst_args_wcc,
}


def run_burst(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    *,
    phase: str,
    nodes: int,
    partitions: int,
    granularity: int,
    memory_mb: int,
    run_index: int,
    graph_file: Path,
) -> dict[str, Any]:
    raw_path = (
        campaign_root / "raw_runs" / "burst"
        / f"{phase}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_ck{args.chunk_size_kb}_run{run_index}.json"
    )
    if raw_path.exists():
        cached = read_json(raw_path)
        if "validation" in cached:
            cached.pop("validation", None)
            write_json(raw_path, cached)
        return cached
    log_path = (
        campaign_root / "logs" / phase
        / f"burst_{algo.name}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_ck{args.chunk_size_kb}_run{run_index}.log"
    )
    ensure_remote_openwhisk_api(args, campaign_root)
    burst_prefix = algo_burst_key_prefix(args, algo)
    args.burst_key_prefix = burst_prefix
    remote_delete_prefix(args, args.bucket, f"{burst_prefix}/{algo.s3_dataset_basename}-{nodes}/output")

    bench_args = BURST_ARG_BUILDERS[algo.name](
        args, algo,
        nodes=nodes, partitions=partitions, granularity=granularity,
        memory_mb=memory_mb, graph_file=graph_file,
    )
    env_parts = [
        "env",
        f"AWS_ACCESS_KEY_ID={shell_quote_env(os.environ['AWS_ACCESS_KEY_ID'])}",
        f"AWS_SECRET_ACCESS_KEY={shell_quote_env(os.environ['AWS_SECRET_ACCESS_KEY'])}",
        f"OW_PROTOCOL={shlex.quote(args.ow_protocol)}",
        f"OPENWHISK_K8S_NAMESPACE={shlex.quote(args.ow_namespace)}",
        f"OPENWHISK_RELEASE_NAME={shlex.quote(args.ow_release_name)}",
    ]
    if algo.name == "wcc":
        env_parts += [
            f"WCC_DATASET_PREFIX={shlex.quote(burst_prefix)}",
            "WCC_DATASET_BASENAME=wcc",
        ]
    remote_command = " ".join([
        "cd", shlex.quote(remote_algo_dir(args, algo)), "&&",
    ] + env_parts + [
        "timeout", f"{int(args.burst_remote_timeout_sec)}s",
    ] + bench_args)

    snap_label = f"burst_{algo.name}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_run{run_index}"
    capture_resource_snapshot(args, campaign_root, phase, f"{snap_label}_pre")

    completed: subprocess.CompletedProcess[str] | None = None
    result: dict[str, Any] | None = None
    for attempt in range(2):
        if attempt > 0:
            ensure_remote_openwhisk_api(args, campaign_root)
        completed = ssh_command(args, remote_command, timeout=3600, log_path=log_path)
        result = None
        if completed.returncode == 0:
            try:
                result = parse_prefixed_json(completed.stdout)
            except ValueError:
                # WCC outputs differently — try parse_json_lines
                try:
                    json_lines = parse_json_lines(completed.stdout)
                    result = next((j for j in json_lines if "end_to_end_ms" in j or "burst_time_s" in j), None)
                except Exception:
                    result = None
            if result is not None:
                break
        if attempt == 0 and has_openwhisk_transport_failure(completed.stdout, completed.stderr):
            time.sleep(1)
            continue
        if completed.returncode != 0:
            raise RuntimeError(f"Burst {algo.name} run failed, see {log_path}")
        raise RuntimeError(f"Burst {algo.name} benchmark returned no parseable result, see {log_path}")

    if completed is None or result is None:
        raise RuntimeError(f"Burst {algo.name} benchmark returned no result, see {log_path}")

    capture_resource_snapshot(args, campaign_root, phase, f"{snap_label}_post")

    record = {
        "phase": phase,
        "framework": "burst",
        "algorithm": algo.name,
        "nodes": nodes,
        "partitions": partitions,
        "granularity": granularity,
        "memory_mb": memory_mb,
        "run_index": run_index,
        "graph_file": str(graph_file),
        "log_path": str(log_path),
        "status": "passed",
        "result": {"burst": result} if "burst" not in result else result,
    }
    write_json(raw_path, record)
    return record


# ---------------------------------------------------------------------------
# Spark benchmark invocation
# ---------------------------------------------------------------------------

def run_spark(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    *,
    phase: str,
    nodes: int,
    partitions: int,
    total_executors: int,
    executor_memory: str,
    run_index: int,
    graph_file: Path,
) -> dict[str, Any]:
    raw_path = (
        campaign_root / "raw_runs" / "spark"
        / f"{phase}_{algo.name}_n{nodes}_e{total_executors}_m{executor_memory}_run{run_index}.json"
    )
    if raw_path.exists():
        cached = read_json(raw_path)
        if "validation" in cached:
            cached.pop("validation", None)
            write_json(raw_path, cached)
        return cached
    workers_6, workers_7 = split_workers(total_executors)
    spark_prefix = algo_spark_key_prefix(args, algo)
    spark_output_prefix = f"{spark_prefix}/n{nodes}/e{total_executors}/mem{executor_memory}/run{run_index}/output"
    spark_input_key = f"{spark_prefix}/inputs/{algo.graph_file_pattern.format(nodes=nodes)}"
    worker_mem_k8s = spark_memory_to_k8s(executor_memory)
    # Pod limit must exceed executor memory by JVM overhead (~10-15%) to avoid
    # OOMKilled. Add 2 GiB headroom on top of executor memory; capped at 8 GiB.
    m = _SPARK_MEMORY_RE.match(executor_memory.strip())
    exec_mem_gi = int(m.group(1)) if m and m.group(2).lower() == "g" else 4
    pod_limit_gi = min(exec_mem_gi + 2, 8)
    worker_pod_limit_k8s = f"{pod_limit_gi}Gi"

    env = os.environ.copy()
    env.update({
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "SPARK_SMOKE_NAMESPACE": args.spark_namespace,
        "SPARK_SMOKE_BUCKET": args.bucket,
        "SPARK_SMOKE_INPUT_KEY": spark_input_key,
        "SPARK_SMOKE_OUTPUT_PREFIX": spark_output_prefix,
        "S3_ENDPOINT": args.host_s3_endpoint,
        "SPARK_MASTER_NODE": args.spark_master_node,
        "SPARK_MASTER_REQUEST_CPU": str(args.spark_master_request_cpu),
        "SPARK_MASTER_LIMIT_CPU": str(args.spark_master_limit_cpu),
        "SPARK_MASTER_REQUEST_MEMORY": args.spark_master_request_memory,
        "SPARK_MASTER_LIMIT_MEMORY": args.spark_master_limit_memory,
        "SPARK_WORKER_COMPUTE6_REPLICAS": str(workers_6),
        "SPARK_WORKER_COMPUTE7_REPLICAS": str(workers_7),
        "SPARK_WORKER_CORES": "1",
        "SPARK_WORKER_MEMORY": executor_memory,
        "SPARK_WORKER_REQUEST_CPU": "1",
        "SPARK_WORKER_LIMIT_CPU": "1",
        "SPARK_WORKER_REQUEST_MEMORY": worker_pod_limit_k8s,
        "SPARK_WORKER_LIMIT_MEMORY": worker_pod_limit_k8s,
        "SPARK_TOTAL_EXECUTOR_CORES": str(total_executors),
        "SPARK_EXECUTOR_CORES": "1",
        "SPARK_EXECUTOR_MEMORY": executor_memory,
        "SPARK_DEFAULT_PARALLELISM": str(total_executors),
        "SPARK_SHUFFLE_PARTITIONS": str(total_executors),
        "SPARK_SKIP_VALIDATION": "true",
    })

    # Algorithm-specific env vars for Spark smoke scripts
    if algo.name == "lp":
        env["LP_SPARK_SMOKE_NODES"] = str(nodes)
        env["LP_SPARK_SMOKE_PARTITIONS"] = str(partitions)
        env["LP_SPARK_SMOKE_ITERATIONS"] = str(args.max_iter)
        env["LP_SPARK_SMOKE_GRAPH_FILE"] = str(graph_file)
    elif algo.name == "bfs":
        env["BFS_SPARK_SMOKE_NODES"] = str(nodes)
        env["BFS_SPARK_SMOKE_PARTITIONS"] = str(partitions)
        env["BFS_SPARK_SMOKE_SOURCE"] = str(args.source_node)
        env["BFS_SPARK_SMOKE_MAX_LEVELS"] = str(args.max_levels)
        env["BFS_SPARK_SMOKE_GRAPH_FILE"] = str(graph_file)
    elif algo.name == "sssp":
        env["SSSP_SPARK_SMOKE_NODES"] = str(nodes)
        env["SSSP_SPARK_SMOKE_PARTITIONS"] = str(partitions)
        env["SSSP_SPARK_SMOKE_SOURCE"] = str(args.source_node)
        env["SSSP_SPARK_SMOKE_MAX_ITER"] = str(args.max_iter)
        env["SSSP_SPARK_SMOKE_GRAPH_FILE"] = str(graph_file)
    elif algo.name == "wcc":
        env["WCC_SPARK_SMOKE_NODES"] = str(nodes)
        env["WCC_SPARK_SMOKE_PARTITIONS"] = str(partitions)
        env["WCC_SPARK_SMOKE_GRAPH_FILE"] = str(graph_file)

    log_path = (
        campaign_root / "logs" / phase
        / f"spark_{algo.name}_n{nodes}_e{total_executors}_m{executor_memory}_run{run_index}.log"
    )
    snap_label = f"spark_{algo.name}_n{nodes}_e{total_executors}_m{executor_memory}_run{run_index}"
    capture_resource_snapshot(args, campaign_root, phase, f"{snap_label}_pre")

    completed = run_command(
        ["bash", str(algo.workdir / algo.spark_smoke_script)],
        cwd=ROOT,
        env=env,
        timeout=7200,
        log_path=log_path,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"Spark {algo.name} run failed, see {log_path}")

    capture_resource_snapshot(args, campaign_root, phase, f"{snap_label}_post")

    json_lines = parse_json_lines(completed.stdout)
    benchmark = next(
        (p for p in json_lines if "end_to_end_ms" in p or "total_time_ms" in p),
        None,
    )
    if benchmark is None:
        raise RuntimeError(f"Spark {algo.name} did not emit benchmark result, see {log_path}")

    load_ms = float(benchmark.get("load_time_ms", 0.0))
    compute_ms = float(benchmark.get("compute_only_ms", benchmark.get("execution_time_ms", 0.0)))
    write_ms = float(benchmark.get("output_write_ms", 0.0))
    end_to_end_ms = float(benchmark.get("end_to_end_ms", benchmark.get("total_time_ms", 0.0)))
    warm_total_ms = load_ms + compute_ms + write_ms
    cold_start_ms = max(0.0, end_to_end_ms - warm_total_ms)
    phase_metrics = {
        "cold_start_ms": cold_start_ms,
        "load_ms": load_ms,
        "compute_ms": compute_ms,
        "write_ms": write_ms,
        "warm_total_ms": warm_total_ms,
        "host_total_ms": end_to_end_ms,
        "iterations": benchmark.get("iterations"),
    }
    record = {
        "phase": phase,
        "framework": "spark",
        "algorithm": algo.name,
        "nodes": nodes,
        "partitions": partitions,
        "executors": total_executors,
        "executor_memory": executor_memory,
        "run_index": run_index,
        "graph_file": str(graph_file),
        "log_path": str(log_path),
        "status": "passed",
        "result": benchmark,
        "phase_metrics": phase_metrics,
    }
    write_json(raw_path, record)
    return record


# ---------------------------------------------------------------------------
# Metric extractors
# ---------------------------------------------------------------------------

def burst_metric(row: dict[str, Any]) -> float:
    r = row.get("result", {})
    burst = r.get("burst", r)
    return burst.get("end_to_end_ms") or burst.get("burst_time_s", 0) * 1000


def spark_metric(row: dict[str, Any]) -> float:
    r = row.get("result", {})
    return r.get("end_to_end_ms") or r.get("total_time_ms", 0)


# ---------------------------------------------------------------------------
# Sync remote scripts
# ---------------------------------------------------------------------------

def sync_remote_scripts(args: argparse.Namespace, algo: AlgorithmConfig) -> None:
    remote_dir = remote_algo_dir(args, algo)
    ssh_command(args, f"mkdir -p {shlex.quote(remote_dir)}")
    for f in [algo.benchmark_script, algo.burst_action_zip, algo.data_generator, "benchmark_runtime_probe.py"]:
        local = algo.workdir / f
        if local.exists():
            scp_to_remote(args, local, f"{remote_dir}/{f}")
    # Also sync shared ow_client if it exists
    ow_client = algo.workdir / "ow_client"
    if ow_client.is_dir():
        ssh_command(args, f"mkdir -p {shlex.quote(remote_dir)}/ow_client")
        for py_file in ow_client.glob("*.py"):
            scp_to_remote(args, py_file, f"{remote_dir}/ow_client/{py_file.name}")
    # Sync any *_utils.py files
    for utils in algo.workdir.glob("*_utils.py"):
        scp_to_remote(args, utils, f"{remote_dir}/{utils.name}")


# ---------------------------------------------------------------------------
# Phase functions (same structure as LP, but algorithm-generic)
# ---------------------------------------------------------------------------

def run_preflight(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    graph_file: Path,
) -> dict[str, Any]:
    burst_row = run_burst(
        args, algo, campaign_root,
        phase="preflight",
        nodes=args.preflight_nodes,
        partitions=args.burst_partitions,
        granularity=args.burst_partitions // 2 if args.burst_partitions >= 4 else args.burst_partitions,
        memory_mb=1024,
        run_index=1,
        graph_file=graph_file,
    )
    payload = {"burst_gate": burst_row}
    write_json(campaign_root / "preflight" / "preflight.json", payload)
    return payload


def run_characterization(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    graph_file: Path,
) -> dict[str, Any]:
    probe_rows: list[dict[str, Any]] = []
    app_rows: list[dict[str, Any]] = []
    granularities = characterization_granularities(args.burst_partitions, algorithm=algo.name)
    burst_prefix = algo_burst_key_prefix(args, algo)
    load_prefix = f"{burst_prefix}/{algo.s3_dataset_basename}-{args.characterization_nodes}"
    for granularity in granularities:
        for mode in args.characterization_probes:
            for run_index in range(1, args.characterization_runs + 1):
                probe_rows.append(
                    run_runtime_probe(
                        args, campaign_root,
                        mode=mode,
                        workers=args.characterization_workers,
                        granularity=granularity,
                        run_index=run_index,
                        load_key_prefix=load_prefix,
                    )
                )
        for run_index in range(1, args.characterization_runs + 1):
            app_rows.append(
                run_burst(
                    args, algo, campaign_root,
                    phase="characterization",
                    nodes=args.characterization_nodes,
                    partitions=args.burst_partitions,
                    granularity=granularity,
                    memory_mb=args.characterization_memory_mb,
                    run_index=run_index,
                    graph_file=graph_file,
                )
            )
    p = args.burst_partitions
    payload = {
        "runtime_probe": summarize_runtime_probes(probe_rows),
        "app_breakdown": summarize_rows(app_rows, metric_extractor=burst_metric),
    }
    write_json(campaign_root / "characterization" / f"runtime_probe_p{p}.json", probe_rows)
    write_json(campaign_root / "characterization" / f"app_runs_p{p}.json", app_rows)
    write_json(campaign_root / "characterization" / f"summary_p{p}.json", payload)
    return payload


def run_config_sweep(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    graph_file: Path,
) -> dict[str, Any]:
    burst_rows: list[dict[str, Any]] = []
    spark_rows: list[dict[str, Any]] = []
    burst_config_matrix = parse_burst_config_matrix(args.burst_partitions)
    budget = args.burst_effective_invokers * args.burst_effective_user_memory_mb
    single_invoker_budget = args.burst_effective_user_memory_mb

    for granularity, memories in burst_config_matrix.items():
        for memory_mb in memories:
            packs = args.burst_partitions // granularity
            raw_base = campaign_root / "raw_runs" / "burst"
            unsafe_total_budget = packs * memory_mb > budget
            unsafe_single_invoker_admission = packs * memory_mb > single_invoker_budget
            if args.burst_partitions % granularity != 0 or unsafe_total_budget or unsafe_single_invoker_admission:
                for run_index in range(1, args.config_runs + 1):
                    raw_path = (
                        raw_base
                        / f"config_sweep_n{args.config_nodes}_p{args.burst_partitions}_g{granularity}_m{memory_mb}_run{run_index}.json"
                    )
                    if raw_path.exists():
                        row = read_json(raw_path)
                    else:
                        if unsafe_total_budget:
                            error = f"exceeds total budget: packs={packs}, mem={memory_mb}, budget={budget}"
                        elif unsafe_single_invoker_admission:
                            error = (
                                "exceeds conservative single-invoker admission guard: "
                                f"packs={packs}, mem={memory_mb}, single_invoker_budget={single_invoker_budget}"
                            )
                        else:
                            error = "partitions must be divisible by granularity"
                        row = failed_run_record(
                            phase="config_sweep", framework="burst", algorithm=algo.name,
                            nodes=args.config_nodes, partitions=args.burst_partitions,
                            granularity=granularity, memory_mb=memory_mb,
                            run_index=run_index, graph_file=str(graph_file),
                            status="blocked",
                            error=error,
                        )
                        write_json(raw_path, row)
                    burst_rows.append(row)
                continue
            for run_index in range(1, args.config_runs + 1):
                raw_path = (
                    raw_base
                    / f"config_sweep_n{args.config_nodes}_p{args.burst_partitions}_g{granularity}_m{memory_mb}_run{run_index}.json"
                )
                try:
                    row = run_burst(
                        args, algo, campaign_root,
                        phase="config_sweep",
                        nodes=args.config_nodes,
                        partitions=args.burst_partitions,
                        granularity=granularity,
                        memory_mb=memory_mb,
                        run_index=run_index,
                        graph_file=graph_file,
                    )
                except Exception as exc:
                    log_path = (
                        campaign_root / "logs" / "config_sweep"
                        / f"burst_{algo.name}_n{args.config_nodes}_p{args.burst_partitions}_g{granularity}_m{memory_mb}_run{run_index}.log"
                    )
                    row = failed_run_record(
                        phase="config_sweep", framework="burst", algorithm=algo.name,
                        nodes=args.config_nodes, partitions=args.burst_partitions,
                        granularity=granularity, memory_mb=memory_mb,
                        run_index=run_index, graph_file=str(graph_file),
                        log_path=str(log_path),
                        error=str(exc),
                    )
                    write_json(raw_path, row)
                burst_rows.append(row)

    if args.burst_partitions in args.spark_partition_executor_map:
        from run_cloudlab_lp_campaign import _set_spark_partition
        _set_spark_partition(args, args.burst_partitions)
        for executor_memory in args.spark_config_memories:
            for run_index in range(1, args.config_runs + 1):
                try:
                    row = run_spark(
                        args, algo, campaign_root,
                        phase="config_sweep",
                        nodes=args.config_nodes,
                        partitions=args.spark_partitions,
                        total_executors=args.spark_total_executors,
                        executor_memory=executor_memory,
                        run_index=run_index,
                        graph_file=graph_file,
                    )
                except Exception as exc:
                    row = failed_run_record(
                        phase="config_sweep", framework="spark", algorithm=algo.name,
                        nodes=args.config_nodes, partitions=args.spark_partitions,
                        executors=args.spark_total_executors,
                        executor_memory=executor_memory,
                        run_index=run_index, graph_file=str(graph_file),
                        error=str(exc),
                    )
                spark_rows.append(row)

    burst_passed = [r for r in burst_rows if r.get("status") == "passed"]
    spark_passed = [r for r in spark_rows if r.get("status") == "passed"]
    burst_summary = summarize_rows(burst_passed, metric_extractor=burst_metric)
    spark_summary = summarize_rows(spark_passed, metric_extractor=spark_metric) if spark_passed else []

    winners: dict[str, Any] = {"burst": pick_best_config(burst_summary)}
    if spark_summary:
        winners["spark"] = pick_best_config(spark_summary)

    p = args.burst_partitions
    write_json(campaign_root / "config_sweep" / f"burst_runs_p{p}.json", burst_rows)
    if spark_rows:
        write_json(campaign_root / "config_sweep" / f"spark_runs_p{p}.json", spark_rows)
    write_json(campaign_root / "config_sweep" / f"burst_summary_p{p}.json", burst_summary)
    write_json(campaign_root / "config_sweep" / f"spark_summary_p{p}.json", spark_summary)
    return winners


def run_size_sweep(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    winners: dict[str, Any],
) -> dict[str, Any]:
    burst_rows: list[dict[str, Any]] = []
    spark_rows: list[dict[str, Any]] = []
    has_spark_winner = "spark" in winners and winners["spark"]

    for nodes in args.size_nodes:
        graph_file = ensure_local_graph_file(args, algo, campaign_root, nodes=nodes, partitions=args.burst_partitions)
        for run_index in range(1, args.size_runs + 1):
            try:
                burst_row = run_burst(
                    args, algo, campaign_root,
                    phase="size_sweep",
                    nodes=nodes,
                    partitions=args.burst_partitions,
                    granularity=int(winners["burst"]["granularity"]),
                    memory_mb=int(winners["burst"]["memory_mb"]),
                    run_index=run_index,
                    graph_file=graph_file,
                )
            except Exception as exc:
                burst_row = failed_run_record(
                    phase="size_sweep", framework="burst", algorithm=algo.name,
                    nodes=nodes, partitions=args.burst_partitions,
                    granularity=int(winners["burst"]["granularity"]),
                    memory_mb=int(winners["burst"]["memory_mb"]),
                    run_index=run_index, graph_file=str(graph_file),
                    error=str(exc),
                )
            burst_rows.append(burst_row)

            if has_spark_winner:
                try:
                    spark_row = run_spark(
                        args, algo, campaign_root,
                        phase="size_sweep",
                        nodes=nodes,
                        partitions=args.spark_partitions,
                        total_executors=int(winners["spark"]["executors"]),
                        executor_memory=str(winners["spark"]["executor_memory"]),
                        run_index=run_index,
                        graph_file=graph_file,
                    )
                except Exception as exc:
                    spark_row = failed_run_record(
                        phase="size_sweep", framework="spark", algorithm=algo.name,
                        nodes=nodes, partitions=args.spark_partitions,
                        executors=int(winners["spark"]["executors"]),
                        executor_memory=str(winners["spark"]["executor_memory"]),
                        run_index=run_index, graph_file=str(graph_file),
                        error=str(exc),
                    )
                spark_rows.append(spark_row)

    burst_passed = [r for r in burst_rows if r.get("status") == "passed"]
    spark_passed = [r for r in spark_rows if r.get("status") == "passed"]
    burst_summary = summarize_rows(burst_passed, metric_extractor=burst_metric)
    spark_summary = summarize_rows(spark_passed, metric_extractor=spark_metric) if spark_passed else []

    p = args.burst_partitions
    payload = {"burst": burst_summary, "spark": spark_summary}
    write_json(campaign_root / "size_sweep" / f"burst_runs_p{p}.json", burst_rows)
    if spark_rows:
        write_json(campaign_root / "size_sweep" / f"spark_runs_p{p}.json", spark_rows)
    write_json(campaign_root / "size_sweep" / f"summary_p{p}.json", payload)
    return payload


def run_chunk_probe(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    winners: dict[str, Any],
) -> dict[str, Any]:
    burst_winner = winners["burst"]
    partitions = int(burst_winner["partitions"])
    granularity = int(burst_winner["granularity"])
    memory_mb = int(burst_winner["memory_mb"])
    nodes = args.config_nodes
    graph_file = ensure_local_graph_file(args, algo, campaign_root, nodes=nodes, partitions=partitions)
    rows: list[dict[str, Any]] = []
    saved_chunk = args.chunk_size_kb
    try:
        for chunk_kb in args.chunk_probe_size_list:
            args.chunk_size_kb = chunk_kb
            for run_index in range(1, args.config_runs + 1):
                try:
                    row = run_burst(
                        args, algo, campaign_root,
                        phase="chunk_probe",
                        nodes=nodes, partitions=partitions,
                        granularity=granularity, memory_mb=memory_mb,
                        run_index=run_index, graph_file=graph_file,
                    )
                    row["chunk_size_kb"] = chunk_kb
                except Exception as exc:
                    row = failed_run_record(
                        phase="chunk_probe", framework="burst", algorithm=algo.name,
                        nodes=nodes, partitions=partitions, granularity=granularity,
                        memory_mb=memory_mb, chunk_size_kb=chunk_kb,
                        run_index=run_index, graph_file=str(graph_file),
                        error=str(exc),
                    )
                rows.append(row)
    finally:
        args.chunk_size_kb = saved_chunk
    write_json(campaign_root / "chunk_probe" / "results.json", rows)
    return {"rows": rows}


# ---------------------------------------------------------------------------
# CLI and main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a CloudLab campaign for a graph algorithm.")
    parser.add_argument("--algorithm", choices=list(ALGORITHMS), required=True)
    parser.add_argument("--campaign-root", type=Path, default=None)
    parser.add_argument("--phase", choices=["full", "preflight", "characterization", "config", "chunk_probe", "size", "report"], default="full")
    parser.add_argument("--cloudlab-user", default="sconde")
    parser.add_argument("--cloudlab-host", default="cloudfunctions.urv.cat")
    parser.add_argument("--cloudlab-ssh-key", default="/home/sergio/.ssh/id_pc1")
    parser.add_argument("--cloudlab-src-root", default=REMOTE_DEFAULT_SRC_ROOT)
    parser.add_argument("--ow-namespace", default="openwhisk")
    parser.add_argument("--ow-release-name", default="owdev")
    parser.add_argument("--ow-host", default="127.0.0.1")
    parser.add_argument("--ow-port", type=int, default=31002)
    parser.add_argument("--ow-protocol", choices=["http", "https"], default="http")
    parser.add_argument("--spark-namespace", default="spark-sconde-smoke")
    parser.add_argument("--spark-master-node", default="compute7")
    parser.add_argument("--bucket", default="tfm-smoke")
    parser.add_argument("--host-s3-endpoint", default="http://192.168.5.24:9000")
    parser.add_argument("--worker-s3-endpoint", default="http://192.168.5.24:9000")
    parser.add_argument("--backend", default="redis-list")
    parser.add_argument("--chunk-size-kb", type=int, default=1024)
    parser.add_argument("--max-iter", type=int, default=500)
    parser.add_argument("--source-node", type=int, default=0)
    parser.add_argument("--max-levels", type=int, default=500)
    parser.add_argument("--preflight-nodes", type=int, default=10_000)
    parser.add_argument("--characterization-nodes", type=int, default=100_000)
    parser.add_argument("--characterization-workers", type=int, default=8)
    parser.add_argument("--characterization-runs", type=int, default=3)
    parser.add_argument("--characterization-memory-mb", type=int, default=1024)
    parser.add_argument("--characterization-iterations", type=int, default=8)
    parser.add_argument("--characterization-payload-bytes", type=int, default=1 << 20)
    parser.add_argument("--characterization-burst-partitions", default=None)
    parser.add_argument("--characterization-probes", default="startup,load,broadcast,all_to_all")
    parser.add_argument("--config-nodes", type=int, default=500_000)
    parser.add_argument("--config-runs", type=int, default=3)
    parser.add_argument("--size-runs", type=int, default=3)
    parser.add_argument("--burst-remote-timeout-sec", type=int, default=1200)
    parser.add_argument("--burst-partitions", default="4,8,16")
    parser.add_argument("--burst-effective-invokers", type=int, default=2)
    parser.add_argument("--burst-effective-user-memory-mb", type=int, default=8192)
    parser.add_argument("--spark-partitions", default="4,8,16")
    parser.add_argument("--spark-total-executors", default="4,8,16")
    parser.add_argument("--spark-config-memories", default="4g,6g")
    parser.add_argument("--size-nodes", default="100000,500000,1000000,2000000")
    parser.add_argument("--chunk-probe-sizes", default="64,256,1024,4096")
    parser.add_argument("--spark-master-request-cpu", default="1")
    parser.add_argument("--spark-master-limit-cpu", default="2")
    parser.add_argument("--spark-master-request-memory", default="2Gi")
    parser.add_argument("--spark-master-limit-memory", default="4Gi")
    args = parser.parse_args()

    if args.campaign_root is None:
        args.campaign_root = EXPERIMENT_ROOT / f"campaign-{args.algorithm}-{utc_stamp()}"

    args.burst_partition_list = [int(t.strip()) for t in args.burst_partitions.split(",") if t.strip()]
    if args.characterization_burst_partitions is None:
        args.characterization_partition_list = list(args.burst_partition_list)
    else:
        args.characterization_partition_list = [
            int(t.strip())
            for t in args.characterization_burst_partitions.split(",")
            if t.strip()
        ]
    args.characterization_probes = [
        token.strip()
        for token in args.characterization_probes.split(",")
        if token.strip()
    ]
    args.spark_partition_list = [int(t.strip()) for t in args.spark_partitions.split(",") if t.strip()]
    args.spark_executor_list = [int(t.strip()) for t in args.spark_total_executors.split(",") if t.strip()]
    args.spark_partition_executor_map: dict[int, int] = {}
    for idx, sp in enumerate(args.spark_partition_list):
        args.spark_partition_executor_map[sp] = args.spark_executor_list[min(idx, len(args.spark_executor_list) - 1)]
    args.spark_config_memories = [t.strip() for t in args.spark_config_memories.split(",") if t.strip()]
    args.size_nodes = [int(t.strip()) for t in args.size_nodes.split(",") if t.strip()]
    args.chunk_probe_size_list = [int(t.strip()) for t in args.chunk_probe_sizes.split(",") if t.strip()]
    args.burst_partitions = args.burst_partition_list[0]
    args.spark_partitions = args.spark_partition_list[0]
    args.spark_total_executors = args.spark_partition_executor_map.get(args.spark_partitions, args.spark_executor_list[0])
    args.remote_ow_port_forward_pid_file = f"/tmp/{args.ow_release_name}-campaign-port-forward.pid"
    args.remote_ow_port_forward_log_file = f"/tmp/{args.ow_release_name}-campaign-port-forward.log"
    return args


def _set_burst_partition(args: argparse.Namespace, partitions: int) -> None:
    args.burst_partitions = partitions
    args.characterization_workers = partitions


def _set_spark_partition(args: argparse.Namespace, partitions: int) -> None:
    args.spark_partitions = partitions
    args.spark_total_executors = args.spark_partition_executor_map.get(
        partitions, args.spark_executor_list[0]
    )


def _load_all_winners(args: argparse.Namespace) -> dict[int, dict[str, Any]]:
    all_winners: dict[int, dict[str, Any]] = {}
    for bp in args.burst_partition_list:
        path = args.campaign_root / "config_sweep" / f"best_config_p{bp}.json"
        if path.exists():
            all_winners[bp] = read_json(path)
    return all_winners


def main() -> None:
    args = parse_args()
    algo = ALGORITHMS[args.algorithm]

    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        raise SystemExit("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for CloudLab MinIO")

    args.campaign_root.mkdir(parents=True, exist_ok=True)
    args.campaign_root = args.campaign_root.resolve()
    metadata = {
        "algorithm": algo.name,
        "campaign_root": str(args.campaign_root),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "burst_partition_list": args.burst_partition_list,
        "characterization_partition_list": args.characterization_partition_list,
        "characterization_probes": args.characterization_probes,
        "spark_partition_list": args.spark_partition_list,
        "spark_partition_executor_map": {str(k): v for k, v in args.spark_partition_executor_map.items()},
        "spark_config_memories": args.spark_config_memories,
        "size_nodes": args.size_nodes,
        "chunk_probe_sizes": args.chunk_probe_size_list,
        "bucket": args.bucket,
        "burst_key_prefix": algo_burst_key_prefix(args, algo),
        "spark_key_prefix": algo_spark_key_prefix(args, algo),
        "host_s3_endpoint": args.host_s3_endpoint,
        "worker_s3_endpoint": args.worker_s3_endpoint,
        "cloudlab_src_root": args.cloudlab_src_root,
    }
    write_json(args.campaign_root / "metadata.json", metadata)

    if args.phase in {"full", "preflight", "characterization", "config", "chunk_probe", "size"}:
        sync_remote_scripts(args, algo)
        ensure_remote_openwhisk_api(args, args.campaign_root)

    # Preflight
    if args.phase in {"full", "preflight"}:
        _set_burst_partition(args, args.burst_partition_list[0])
        graph_file = ensure_local_graph_file(
            args, algo, args.campaign_root,
            nodes=args.preflight_nodes, partitions=args.burst_partitions,
        )
        run_preflight(args, algo, args.campaign_root, graph_file)

    # Characterization per partition
    all_winners: dict[int, dict[str, Any]] = {}
    for bp in args.characterization_partition_list:
        _set_burst_partition(args, bp)
        if args.phase in {"full", "characterization"}:
            graph_file = ensure_local_graph_file(
                args, algo, args.campaign_root,
                nodes=args.characterization_nodes, partitions=bp,
            )
            run_characterization(args, algo, args.campaign_root, graph_file)

    # Config sweep per partition
    for bp in args.burst_partition_list:
        _set_burst_partition(args, bp)
        if args.phase in {"full", "config"}:
            graph_file = ensure_local_graph_file(
                args, algo, args.campaign_root,
                nodes=args.config_nodes, partitions=bp,
            )
            winners = run_config_sweep(args, algo, args.campaign_root, graph_file)
            write_json(args.campaign_root / "config_sweep" / f"best_config_p{bp}.json", winners)
            all_winners[bp] = winners

    # Chunk probe
    if args.phase in {"full", "chunk_probe"}:
        if not all_winners:
            all_winners = _load_all_winners(args)
        first_bp = args.burst_partition_list[0]
        if first_bp in all_winners:
            _set_burst_partition(args, first_bp)
            run_chunk_probe(args, algo, args.campaign_root, all_winners[first_bp])

    # Size sweep per partition
    if args.phase in {"full", "size"}:
        if not all_winners:
            all_winners = _load_all_winners(args)
        for bp in args.burst_partition_list:
            _set_burst_partition(args, bp)
            if bp not in all_winners:
                path = args.campaign_root / "config_sweep" / f"best_config_p{bp}.json"
                if not path.exists():
                    raise RuntimeError(f"missing config_sweep/best_config_p{bp}.json; run config phase first")
                all_winners[bp] = read_json(path)
            if bp in args.spark_partition_executor_map:
                _set_spark_partition(args, bp)
            else:
                _set_spark_partition(args, args.spark_partition_list[0])
            run_size_sweep(args, algo, args.campaign_root, all_winners[bp])

    # Combined best_config
    if args.phase in {"full", "config"} and all_winners:
        write_json(
            args.campaign_root / "config_sweep" / "best_config.json",
            {str(bp): w for bp, w in all_winners.items()},
        )

    print(f"\n{'='*60}")
    print(f"Campaign {algo.display_name} complete: {args.campaign_root}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
