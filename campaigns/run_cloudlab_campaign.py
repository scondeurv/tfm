#!/usr/bin/env python3
"""Generalized CloudLab campaign runner for multiple graph algorithms.

Supports LP, BFS, SSSP using the same phase structure:
  preflight → characterization → config_sweep → chunk_probe → size_sweep → report

Algorithm-specific behavior is encapsulated in AlgorithmConfig.
Reuses infrastructure from run_cloudlab_lp_campaign.py.
"""
from __future__ import annotations

import argparse
import atexit
import dataclasses
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Import shared infrastructure from common module
from cloudlab_common import (
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
    read_cached_record,
    cached_record_ok,
    summarize_runtime_probes,
    pick_best_config,
    parse_burst_config_matrix,
    ensure_remote_openwhisk_api,
    run_runtime_probe,
    mean_std,
    burst_memory_mb,
    burst_cell_fit,
    spark_cell_fit,
    cloudlab_node_budget,
)

from preflight_gate import ensure_cluster_ready, PreflightError

from cost_backends import (
    COST_BACKEND_CONFIGS,
    ensure_remote_graph_file,
    expand_cost_cells,
    mpi_extra_hosts,
    propagate_remote_file,
    run_mpi_remote,
    run_rayon_remote,
    run_standalone_remote,
)

LP_DIR = ROOT / "labelpropagation"
BFS_DIR = ROOT / "bfs"
SSSP_DIR = ROOT / "sssp"
EXPERIMENT_ROOT = ROOT / "experiment_data" / "cloudlab_campaigns"


@dataclasses.dataclass
class AlgorithmConfig:
    name: str                       # "lp", "bfs", "sssp", "pagerank"
    display_name: str               # "Label Propagation", "BFS", ...
    workdir: Path                   # LP_DIR, BFS_DIR, ...
    benchmark_script: str           # "benchmark_lp.py", "benchmark_bfs.py"
    spark_smoke_script: str         # "run_cloudlab_smoke_lp_spark.sh"
    data_generator: str             # "setup_large_lp_data.py"
    graph_file_pattern: str         # "large_lp_{nodes}.txt"
    s3_prefix_infix: str            # "lp", "bfs", "sssp"
    s3_dataset_basename: str        # "large", "large-bfs", "large-sssp" — must match {key_prefix}/{basename}-{nodes} in benchmark_*.py
    burst_action_zip: str           # "labelpropagation.zip", "bfs.zip", ...
    extra_data_gen_args: list[str] = dataclasses.field(default_factory=list)
    # When False, the orchestrator skips burst + spark phases for this algorithm
    # because the OpenWhisk action / Spark submitter has not been implemented yet.
    has_burst_action: bool = True
    has_spark_submitter: bool = True


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
    "pagerank": AlgorithmConfig(
        name="pagerank",
        display_name="PageRank",
        workdir=ROOT / "pagerank",
        benchmark_script="benchmark_pagerank.py",
        spark_smoke_script="run_cloudlab_smoke_pagerank_spark.sh",
        data_generator="setup_large_pagerank_data.py",
        graph_file_pattern="large_pagerank_{nodes}.txt",
        s3_prefix_infix="pagerank",
        s3_dataset_basename="large-pagerank",
        burst_action_zip="pagerank.zip",
        has_burst_action=True,
        has_spark_submitter=True,
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

    # External (real-world) dataset path: when the user passes
    # --external-graph-tsv, we treat that file as the canonical graph for the
    # configured --external-graph-num-nodes value and skip synthetic generation.
    external_path = getattr(args, "external_graph_tsv", None)
    external_n = getattr(args, "external_graph_num_nodes", None)
    if external_path and external_n and nodes == external_n:
        src = Path(external_path).resolve()
        if not src.exists():
            raise RuntimeError(f"--external-graph-tsv path does not exist: {src}")
        # Symlink (or copy) into the campaign datasets dir so downstream stages
        # find it via the expected pattern.
        graph_file = dataset_dir / algo.graph_file_pattern.format(nodes=nodes)
        if not graph_file.exists():
            try:
                graph_file.symlink_to(src)
            except OSError:
                import shutil
                shutil.copy2(src, graph_file)
        # Skip S3 partition upload when no Burst/Spark phases are active —
        # COST-only sweeps don't need the per-partition S3 shards, so a
        # locally-symlinked external dataset with an empty stub is fine.
        burst_active = bool(set(getattr(args, "backend_list", [])) & {"burst", "spark"})
        if burst_active:
            burst_prefix = f"{algo_burst_key_prefix(args, algo)}/{algo.s3_dataset_basename}-{nodes}"
            if not remote_lp_partitions_available(args, args.bucket, burst_prefix, partitions):
                upload_partitions_via_proxy(args, algo, campaign_root, graph_file, args.bucket, burst_prefix, partitions)
        return graph_file

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
        cmd.extend(["--density", "10", "--no-s3"])
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
    # Skip SCP when remote already has a non-empty file/symlink at the
    # expected path — supports staging real-world datasets (e.g. SNAP TSVs)
    # directly on CloudLab without re-uploading multi-GB files from local.
    probe = ssh_command(args, f"test -s {shlex.quote(remote_graph)} && echo OK || echo MISSING")
    if probe.returncode != 0 or "OK" not in probe.stdout:
        upload_completed = scp_to_remote(args, graph_file, remote_graph)
        if upload_completed.returncode != 0:
            raise RuntimeError(f"failed to upload graph file to CloudLab\nSTDERR:\n{upload_completed.stderr}")

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

def _burst_args_lp(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file, skip_clean: bool = True, skip_action_update: bool = False) -> list[str]:
    load_prefix = f"{algo_burst_key_prefix(args, algo)}/large-{nodes}"
    cmd = [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--iter", str(args.max_iter),
        "--memory", str(memory_mb),
        "--ow-host", shlex.quote(args.ow_host),
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
        "--validation-endpoint", shlex.quote(args.host_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]
    if skip_clean:
        cmd.append("--skip-clean")
    if skip_action_update:
        # Warm shot: keep the pool valid — re-registering the action would bump
        # its revision and force cold starts, defeating the warm-pool protocol.
        cmd.append("--skip-action-update")
    return cmd


def _burst_args_bfs(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file, skip_clean: bool = True, skip_action_update: bool = False) -> list[str]:
    cmd = [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--source", str(args.source_node),
        "--max-levels", str(args.max_levels),
        "--memory", str(memory_mb),
        "--ow-host", shlex.quote(args.ow_host),
        "--ow-port", str(args.ow_port),
        "--skip-standalone",
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--validation-endpoint", shlex.quote(args.host_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]
    if skip_clean:
        cmd.append("--skip-clean")
    if skip_action_update:
        # Warm shot: keep the pool valid — re-registering the action would bump
        # its revision and force cold starts, defeating the warm-pool protocol.
        cmd.append("--skip-action-update")
    return cmd


def _burst_args_sssp(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file, skip_clean: bool = True, skip_action_update: bool = False) -> list[str]:
    cmd = [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--source", str(args.source_node),
        "--max-iterations", str(args.max_iter),
        "--memory", str(memory_mb),
        "--ow-host", shlex.quote(args.ow_host),
        "--ow-port", str(args.ow_port),
        "--skip-standalone",
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--validation-endpoint", shlex.quote(args.host_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]
    if skip_clean:
        cmd.append("--skip-clean")
    if skip_action_update:
        # Warm shot: keep the pool valid — re-registering the action would bump
        # its revision and force cold starts, defeating the warm-pool protocol.
        cmd.append("--skip-action-update")
    return cmd


def _burst_args_pagerank(args: argparse.Namespace, algo: AlgorithmConfig, *, nodes, partitions, granularity, memory_mb, graph_file, skip_clean: bool = True, skip_action_update: bool = False) -> list[str]:
    cmd = [
        "python3", algo.benchmark_script,
        "--nodes", str(nodes),
        "--partitions", str(partitions),
        "--granularity", str(granularity),
        "--max-iterations", str(args.max_iter),
        "--memory", str(memory_mb),
        "--ow-host", shlex.quote(args.ow_host),
        "--ow-port", str(args.ow_port),
        "--ow-protocol", shlex.quote(args.ow_protocol),
        "--skip-standalone",
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
        "--validation-endpoint", shlex.quote(args.host_s3_endpoint),
        "--bucket", shlex.quote(args.bucket),
        "--key-prefix", shlex.quote(algo_burst_key_prefix(args, algo)),
    ]
    if skip_clean:
        cmd.append("--skip-clean")
    if skip_action_update:
        # Warm shot: keep the pool valid — re-registering the action would bump
        # its revision and force cold starts, defeating the warm-pool protocol.
        cmd.append("--skip-action-update")
    return cmd


BURST_ARG_BUILDERS: dict[str, Callable] = {
    "lp": _burst_args_lp,
    "bfs": _burst_args_bfs,
    "sssp": _burst_args_sssp,
    "pagerank": _burst_args_pagerank,
}


_ACTIVATION_DURATION_RE = re.compile(r"'duration':\s*(\d+),\s*'end':\s*\d+")


def extract_burst_compute_only_proxy(log_path: Path) -> int | None:
    """Parse OpenWhisk activation durations from a Burst invocation log.

    Used when the Burst backend (e.g. redis-list for BFS/SSSP) does not
    instrument ``compute_only_ms`` in the host-side phase_metrics. The proxy is
    ``max(activation.duration)`` across all workers in the run — the slowest
    worker's compute time, excluding OpenWhisk queueing (``waitTime``) and
    cold-container init (``initTime``). It still includes per-worker S3
    partition load and any intra-iteration Redis I/O.

    Returns the proxy in milliseconds, or ``None`` if the log has no
    parseable activation records.
    """
    try:
        text = log_path.read_text(errors="replace")
    except OSError:
        return None
    durations = [int(m) for m in _ACTIVATION_DURATION_RE.findall(text)]
    if not durations:
        return None
    return int(max(durations))


def apply_compute_only_proxy(record: dict[str, Any], log_path: Path) -> dict[str, Any]:
    """Inject a compute_only proxy into a Burst record when the backend left it null.

    Mutates ``record['result']['burst']`` in place: writes
    ``compute_only_ms_proxy`` (always, when parseable) and populates
    ``compute_only_ms`` only if it was missing or ``None``. Also injects the
    same value into ``phase_metrics.compute_ms`` if that field is missing/None.
    Returns the (possibly mutated) record.
    """
    burst = record.get("result", {}).get("burst")
    if not isinstance(burst, dict):
        return record
    proxy = extract_burst_compute_only_proxy(log_path)
    if proxy is None:
        return record
    burst["compute_only_ms_proxy"] = proxy
    burst["compute_only_ms_proxy_source"] = "max_activation_duration"
    if burst.get("compute_only_ms") is None:
        burst["compute_only_ms"] = proxy
    pm = burst.get("phase_metrics")
    if isinstance(pm, dict) and pm.get("compute_ms") is None:
        pm["compute_ms"] = proxy
    return record


def _invoke_burst_shot(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    *,
    phase: str,
    nodes: int,
    partitions: int,
    granularity: int,
    memory_mb: int,
    graph_file: Path,
    log_path: Path,
    skip_clean: bool,
    skip_action_update: bool = False,
) -> dict[str, Any]:
    """Run a single Burst benchmark invocation via SSH and return parsed result.

    `skip_clean=False` reuses no pods (cold). `skip_clean=True` keeps the OW pool
    alive (warm). Caller decides which mode and how many shots to run.
    """
    ensure_remote_openwhisk_api(args, campaign_root)
    burst_prefix = algo_burst_key_prefix(args, algo)
    args.burst_key_prefix = burst_prefix
    remote_delete_prefix(args, args.bucket, f"{burst_prefix}/{algo.s3_dataset_basename}-{nodes}/output")

    bench_args = BURST_ARG_BUILDERS[algo.name](
        args, algo,
        nodes=nodes, partitions=partitions, granularity=granularity,
        memory_mb=memory_mb, graph_file=graph_file,
        skip_clean=skip_clean, skip_action_update=skip_action_update,
    )
    env_parts = [
        "env",
        f"AWS_ACCESS_KEY_ID={shell_quote_env(os.environ['AWS_ACCESS_KEY_ID'])}",
        f"AWS_SECRET_ACCESS_KEY={shell_quote_env(os.environ['AWS_SECRET_ACCESS_KEY'])}",
        f"OW_PROTOCOL={shlex.quote(args.ow_protocol)}",
        f"OPENWHISK_K8S_NAMESPACE={shlex.quote(args.ow_namespace)}",
        f"OPENWHISK_RELEASE_NAME={shlex.quote(args.ow_release_name)}",
        f"PYTHONPATH={shlex.quote(args.cloudlab_src_root + '/labelpropagation')}",
    ]
    remote_command = " ".join([
        "cd", shlex.quote(remote_algo_dir(args, algo)), "&&",
    ] + env_parts + [
        "timeout", f"{int(args.burst_remote_timeout_sec)}s",
    ] + bench_args)

    completed: subprocess.CompletedProcess[str] | None = None
    result: dict[str, Any] | None = None
    for attempt in range(2):
        if attempt > 0:
            ensure_remote_openwhisk_api(args, campaign_root)
        completed = ssh_command(args, remote_command, timeout=3600, log_path=log_path)
        result = None
        try:
            result = parse_prefixed_json(completed.stdout)
        except ValueError:
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
    return result


def _extract_shot_metrics(shot: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return (end_to_end_ms, warm_subtotal_ms) from a parsed Burst shot result.

    `warm_subtotal_ms` here = load+compute+communication+write (the steady-state
    work without cold-start overhead). Benchmark emits this as either
    `burst.warm_total_ms` (top-level) or `burst.phase_metrics.warm_total_ms`.
    """
    burst = shot.get("burst") if "burst" in shot else shot
    if not isinstance(burst, dict):
        return None, None
    e2e = burst.get("end_to_end_ms")
    subtotal = burst.get("warm_total_ms")
    if not isinstance(subtotal, (int, float)):
        pm = burst.get("phase_metrics") or {}
        subtotal = pm.get("warm_total_ms") if isinstance(pm, dict) else None
    return (
        float(e2e) if isinstance(e2e, (int, float)) else None,
        float(subtotal) if isinstance(subtotal, (int, float)) else None,
    )


def _validate_burst_shot(
    shot: dict[str, Any],
    *,
    algo: AlgorithmConfig,
    phase: str,
    nodes: int,
    partitions: int,
    label: str,
) -> None:
    burst = shot.get("burst") if isinstance(shot, dict) and "burst" in shot else shot
    if not isinstance(burst, dict):
        raise RuntimeError(f"Burst {algo.name} {phase} n={nodes} {label}: missing burst payload")

    pm = burst.get("phase_metrics") if isinstance(burst.get("phase_metrics"), dict) else {}
    e2e, subtotal = _extract_shot_metrics(shot)
    compute = burst.get("compute_only_ms")
    if not isinstance(compute, (int, float)):
        compute = burst.get("processing_time_ms")
    if not isinstance(compute, (int, float)):
        compute = pm.get("span_ms")
    iterations = pm.get("iterations")
    workers = pm.get("workers")

    reasons: list[str] = []
    if e2e is None:
        reasons.append("missing end_to_end_ms")
    if subtotal is None:
        reasons.append("missing warm_total_ms")
    if not isinstance(compute, (int, float)):
        reasons.append("missing compute metric")
    if not isinstance(iterations, int) or iterations <= 0:
        reasons.append(f"invalid iterations={iterations!r}")
    if isinstance(workers, int) and workers != partitions:
        reasons.append(f"workers={workers} != partitions={partitions}")
    if isinstance(shot, dict):
        response_status = shot.get("response", {}).get("status") if isinstance(shot.get("response"), dict) else None
        if response_status and response_status != "success":
            reasons.append(f"response_status={response_status}")

    if reasons:
        raise RuntimeError(
            f"Burst {algo.name} {phase} n={nodes} p={partitions} {label} invalid: "
            + "; ".join(reasons)
        )


def _validate_burst_warmpool_cache(
    campaign_root: Path,
    *,
    algo: AlgorithmConfig,
    phase: str,
    nodes: int,
    partitions: int,
    granularity: int,
    memory_mb: int,
    chunk_size_kb: int,
    run_index: int,
    warmup_shots: int,
) -> None:
    """Validate cached warm-pool sidecars before trusting an aggregate raw run."""
    base_name = (
        f"{algo.name}_{phase}_n{nodes}_p{partitions}_g{granularity}"
        f"_m{memory_mb}_ck{chunk_size_kb}_run{run_index}"
    )
    warm_dir = campaign_root / "raw_runs" / "burst_warm"
    required = [("cold", warm_dir / f"{base_name}_cold.json")]
    required.extend(
        (f"warm_rep{i}", warm_dir / f"{base_name}_warm_rep{i}.json")
        for i in range(1, warmup_shots + 1)
    )
    for label, path in required:
        if not path.exists():
            raise RuntimeError(f"missing warm-pool sidecar {path}")
        _validate_burst_shot(
            read_json(path),
            algo=algo,
            phase=phase,
            nodes=nodes,
            partitions=partitions,
            label=f"run{run_index}_{label}_cache",
        )


def _median(values: list[float]) -> float | None:
    finite = [v for v in values if v is not None]
    if not finite:
        return None
    s = sorted(finite)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 == 1 else (s[mid - 1] + s[mid]) / 2.0


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
        / f"{algo.name}_{phase}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_ck{args.chunk_size_kb}_run{run_index}.json"
    )
    log_path = (
        campaign_root / "logs" / phase
        / f"burst_{algo.name}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_ck{args.chunk_size_kb}_run{run_index}.log"
    )
    warmup_shots = int(getattr(args, "burst_warmup_shots", 0) or 0)
    warmup_override_map = getattr(args, "burst_warmup_shots_override_map", {}) or {}
    if nodes in warmup_override_map:
        warmup_shots = int(warmup_override_map[nodes])

    if raw_path.exists():
        cached = read_json(raw_path)
        mutated = False
        if "validation" in cached:
            cached.pop("validation", None)
            mutated = True
        burst_cached = cached.get("result", {}).get("burst") if isinstance(cached.get("result"), dict) else None
        if isinstance(burst_cached, dict) and "compute_only_ms_proxy" not in burst_cached and log_path.exists():
            before = json.dumps(burst_cached, sort_keys=True)
            apply_compute_only_proxy(cached, log_path)
            if json.dumps(cached.get("result", {}).get("burst", {}), sort_keys=True) != before:
                mutated = True
        # Warm-pool freshness: legacy cache (no `warmpool` block) is treated as
        # stale when warm-pool is enabled. Cached aggregates and warm sidecars
        # are validated before reuse, so aborted/partial campaign artifacts
        # cannot silently enter summaries.
        try:
            if warmup_shots > 0 and "warmpool" not in cached:
                raise RuntimeError("missing warmpool block")
            cached_result = cached.get("result", cached)
            _validate_burst_shot(
                cached_result,
                algo=algo,
                phase=phase,
                nodes=nodes,
                partitions=partitions,
                label=f"run{run_index}_cache",
            )
            if warmup_shots > 0:
                _validate_burst_warmpool_cache(
                    campaign_root,
                    algo=algo,
                    phase=phase,
                    nodes=nodes,
                    partitions=partitions,
                    granularity=granularity,
                    memory_mb=memory_mb,
                    chunk_size_kb=args.chunk_size_kb,
                    run_index=run_index,
                    warmup_shots=warmup_shots,
                )
        except Exception as exc:
            print(f"[burst] cache invalid for {raw_path.name}: {exc}; regenerating")
        else:
            if mutated:
                write_json(raw_path, cached)
            return cached

    snap_label = f"burst_{algo.name}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_run{run_index}"
    capture_resource_snapshot(args, campaign_root, phase, f"{snap_label}_pre")

    if warmup_shots <= 0:
        # Legacy single-shot path (cold; --skip-clean still passed historically).
        result = _invoke_burst_shot(
            args, algo, campaign_root,
            phase=phase, nodes=nodes, partitions=partitions,
            granularity=granularity, memory_mb=memory_mb,
            graph_file=graph_file, log_path=log_path,
            skip_clean=True,
        )
        _validate_burst_shot(
            result, algo=algo, phase=phase, nodes=nodes,
            partitions=partitions, label=f"run{run_index}",
        )
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
        apply_compute_only_proxy(record, log_path)
        write_json(raw_path, record)
        return record

    # Warm-pool protocol: 1 cold discarded + N warm reps. The cold shot resets
    # the OW pool; the N warm shots reuse it (--skip-clean). Median of warm
    # end_to_end_ms replaces the legacy single-shot mean_end_to_end_ms.
    warm_dir = campaign_root / "raw_runs" / "burst_warm"
    warm_dir.mkdir(parents=True, exist_ok=True)
    base_name = f"{algo.name}_{phase}_n{nodes}_p{partitions}_g{granularity}_m{memory_mb}_ck{args.chunk_size_kb}_run{run_index}"
    cold_log = campaign_root / "logs" / phase / f"burst_{base_name}_cold.log"
    cold_result = _invoke_burst_shot(
        args, algo, campaign_root,
        phase=phase, nodes=nodes, partitions=partitions,
        granularity=granularity, memory_mb=memory_mb,
        graph_file=graph_file, log_path=cold_log,
        skip_clean=False,
    )
    write_json(warm_dir / f"{base_name}_cold.json", cold_result)
    _validate_burst_shot(
        cold_result, algo=algo, phase=phase, nodes=nodes,
        partitions=partitions, label=f"run{run_index}_cold",
    )
    cold_e2e, _ = _extract_shot_metrics(cold_result)

    warm_results: list[dict[str, Any]] = []
    warm_e2e_values: list[float] = []
    warm_subtotal_values: list[float] = []
    for i in range(1, warmup_shots + 1):
        warm_log = campaign_root / "logs" / phase / f"burst_{base_name}_warm_rep{i}.log"
        shot = _invoke_burst_shot(
            args, algo, campaign_root,
            phase=phase, nodes=nodes, partitions=partitions,
            granularity=granularity, memory_mb=memory_mb,
            graph_file=graph_file, log_path=warm_log,
            skip_clean=True,
            skip_action_update=True,
        )
        write_json(warm_dir / f"{base_name}_warm_rep{i}.json", shot)
        _validate_burst_shot(
            shot, algo=algo, phase=phase, nodes=nodes,
            partitions=partitions, label=f"run{run_index}_warm_rep{i}",
        )
        warm_results.append(shot)
        e2e, subtotal = _extract_shot_metrics(shot)
        if e2e is not None:
            warm_e2e_values.append(e2e)
        if subtotal is not None:
            warm_subtotal_values.append(subtotal)

    warm_e2e_median = _median(warm_e2e_values)
    warm_subtotal_median = _median(warm_subtotal_values)

    # Use the final warm shot's `burst` sub-dict as the canonical `result.burst`
    # so downstream readers (e.g. report_generators._load_size_sweep_burst) see
    # a complete schema. Then overwrite end_to_end_ms / warm_subtotal_ms with
    # the medians of the warm population.
    canonical_shot = warm_results[-1]
    canonical_burst = (
        canonical_shot.get("burst") if isinstance(canonical_shot, dict) and "burst" in canonical_shot
        else canonical_shot
    )
    if isinstance(canonical_burst, dict):
        canonical_burst = dict(canonical_burst)
        if warm_e2e_median is not None:
            canonical_burst["end_to_end_ms"] = warm_e2e_median
        if warm_subtotal_median is not None:
            canonical_burst["warm_subtotal_ms"] = warm_subtotal_median
    result_block: dict[str, Any] = {"burst": canonical_burst}

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
        "result": result_block,
        "warmpool": {
            "shots": warmup_shots,
            "cold_e2e_ms": cold_e2e,
            "warm_e2e_ms": warm_e2e_values,
            "warm_subtotal_ms": warm_subtotal_values,
            "warm_e2e_median_ms": warm_e2e_median,
            "warm_subtotal_median_ms": warm_subtotal_median,
        },
    }
    # The last warm log is the canonical proxy source (warm pool, no cold spike).
    last_warm_log = campaign_root / "logs" / phase / f"burst_{base_name}_warm_rep{warmup_shots}.log"
    apply_compute_only_proxy(record, last_warm_log)
    write_json(raw_path, record)
    return record


# ---------------------------------------------------------------------------
# Spark benchmark invocation
# ---------------------------------------------------------------------------

def _ssh_config_args(args: argparse.Namespace) -> list[str]:
    cfg = getattr(args, "cloudlab_ssh_config", None) or os.environ.get("CLOUDLAB_SSH_CONFIG")
    return ["-F", cfg] if cfg else []


def _ssh_kubectl(
    args: argparse.Namespace, remote_cmd: str, timeout: int = 120
) -> subprocess.CompletedProcess[str]:
    """SSH to the Spark kubectl host and run `remote_cmd`. Best-effort."""
    kube_host = getattr(args, "spark_kubectl_host", None) or "cloudfunctions.urv.cat"
    key = getattr(args, "cloudlab_ssh_key", None)
    user = getattr(args, "cloudlab_user", "sconde")
    cmd = [
        "ssh", *_ssh_config_args(args),
        "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=15",
    ]
    if key:
        cmd += ["-i", key]
    cmd += [f"{user}@{kube_host}", remote_cmd]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)


def _spark_kill_app_by_name(args: argparse.Namespace, app_name: str) -> None:
    """Kill a Spark application by its --name on the master so executors release.

    Two-step: (1) curl /json/ from inside the master pod, parse JSON locally
    to find the app_id; (2) re-enter the pod and issue spark-submit --kill.
    Done locally (not inside remote bash) to avoid quote-escaping hell —
    the spark master image lacks python3 + jq for in-pod parsing.

    Tolerant: best-effort, never raises. Falls back to pkill if anything fails.
    """
    ns = getattr(args, "spark_namespace", "spark-sconde-smoke")
    fetch = (
        f"kubectl -n {shlex.quote(ns)} exec deploy/spark-master -c spark-master "
        f"-- curl -fsS http://spark-master:8080/json/"
    )
    try:
        result = _ssh_kubectl(args, fetch, timeout=30)
        if result.returncode != 0 or not result.stdout.strip():
            print(f"[spark] kill-by-name {app_name}: fetch /json/ rc={result.returncode}; pkill fallback")
            _spark_kill_remote_shell(args)
            return
        import json as _json
        try:
            data = _json.loads(result.stdout)
        except _json.JSONDecodeError as exc:
            print(f"[spark] kill-by-name {app_name}: JSON parse failed ({exc}); pkill fallback")
            _spark_kill_remote_shell(args)
            return
        active = data.get("activeapps") or []
        app_id = next((a.get("id") for a in active if a.get("name") == app_name), None)
        if not app_id:
            print(f"[spark] kill-by-name {app_name}: no active app matched (active={len(active)})")
            return
        kill = (
            f"kubectl -n {shlex.quote(ns)} exec deploy/spark-master -c spark-master "
            f"-- /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
            f"--kill {shlex.quote(app_id)}"
        )
        kr = _ssh_kubectl(args, kill, timeout=60)
        if kr.returncode == 0:
            print(f"[spark] killed app_id={app_id} (name={app_name})")
        else:
            print(f"[spark] spark-submit --kill {app_id} rc={kr.returncode}; pkill fallback")
            _spark_kill_remote_shell(args)
    except Exception as exc:  # noqa: BLE001
        print(f"[spark] kill-by-name {app_name} failed ({exc}); pkill fallback")
        _spark_kill_remote_shell(args)


def _spark_wait_for_health(args: argparse.Namespace, timeout: int = 120) -> bool:
    """Poll until the spark-master pod is Ready. Return True on success."""
    ns = getattr(args, "spark_namespace", "spark-sconde-smoke")
    remote = (
        f"kubectl -n {shlex.quote(ns)} get pods -l app=spark-master "
        f"-o jsonpath='{{range .items[*]}}{{.status.conditions[?(@.type==\"Ready\")].status}}{{\"\\n\"}}{{end}}'"
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = _ssh_kubectl(args, remote, timeout=30)
            if result.returncode == 0 and "True" in result.stdout:
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"[spark] master not Ready after {timeout}s; consider rollout restart")
    return False


def _spark_rollout_restart(args: argparse.Namespace) -> None:
    """Restart spark-master + workers and wait for rollout to converge."""
    ns = getattr(args, "spark_namespace", "spark-sconde-smoke")
    deploys = "deploy/spark-master deploy/spark-worker-compute6 deploy/spark-worker-compute7"
    remote = (
        f"kubectl -n {shlex.quote(ns)} rollout restart {deploys} && "
        f"kubectl -n {shlex.quote(ns)} rollout status {deploys} --timeout=180s"
    )
    try:
        result = _ssh_kubectl(args, remote, timeout=240)
        if result.returncode == 0:
            print(f"[spark] rollout restart OK")
        else:
            print(f"[spark] rollout restart rc={result.returncode}: {result.stderr[:200]}")
    except Exception as exc:  # noqa: BLE001
        print(f"[spark] rollout restart failed (ignored): {exc}")


_SPARK_CONSECUTIVE_TIMEOUTS = {"count": 0}


def _spark_kill_remote_shell(args: argparse.Namespace) -> None:
    """Best-effort cleanup: kill any spark-shell/SparkSubmit left running in the
    Spark master pod after a cell timeout, so the next cell starts from a clean
    master (a `kubectl apply` redeploy does NOT recreate a healthy master pod,
    so an orphaned JVM would otherwise linger). Never raises.

    kubectl lives on the proxy/control-plane host, not on cloudlab_host
    (compute6), so this SSHes to --spark-kubectl-host explicitly.
    """
    ns = getattr(args, "spark_namespace", "spark-sconde-smoke")
    kube_host = getattr(args, "spark_kubectl_host", None) or "cloudfunctions.urv.cat"
    key = getattr(args, "cloudlab_ssh_key", None)
    user = getattr(args, "cloudlab_user", "sconde")
    try:
        remote = (
            f"kubectl -n {shlex.quote(ns)} exec deploy/spark-master -- "
            f"pkill -f 'spark-shell|SparkSubmit' || true"
        )
        cmd = [
            "ssh", *_ssh_config_args(args),
            "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=15",
        ]
        if key:
            cmd += ["-i", key]
        cmd += [f"{user}@{kube_host}", remote]
        subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
        print(f"[spark] best-effort kill of stale spark-shell in ns={ns} done")
    except Exception as exc:  # noqa: BLE001 - cleanup must never break the sweep
        print(f"[spark] cleanup best-effort failed (ignored): {exc}")


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
    # M4 Nivel 2: per-n executor memory override (e.g. n=5M → 12g) applied
    # before the raw_path cache key so reruns with a bumped override write a
    # distinct record. raw_path itself already encodes executor_memory.
    exec_mem_override_map = getattr(args, "spark_executor_memory_override_map", {}) or {}
    if nodes in exec_mem_override_map:
        executor_memory = str(exec_mem_override_map[nodes])
        raw_path = (
            campaign_root / "raw_runs" / "spark"
            / f"{phase}_{algo.name}_n{nodes}_e{total_executors}_m{executor_memory}_run{run_index}.json"
        )
    if raw_path.exists():
        cached = read_cached_record(raw_path)
        if cached is not None and cached_record_ok(cached):
            if "validation" in cached:
                cached.pop("validation", None)
                write_json(raw_path, cached)
            return cached
        print(f"[spark] cache invalid for {raw_path.name}; regenerating")
    # M4 Nivel 2: per-n cell timeout override (e.g. n=5M → 900s hard cap).
    cell_timeout = int(getattr(args, "spark_cell_timeout_sec", 5400) or 5400)
    timeout_override_map = getattr(args, "spark_size_timeout_override_map", {}) or {}
    timeout_overridden = nodes in timeout_override_map
    if timeout_overridden:
        cell_timeout = int(timeout_override_map[nodes])
    # M4: pre-cell health check. If master not Ready (compute7 worker
    # ContainerStatusUnknown was the 0529 failure mode) restart and retry.
    if not _spark_wait_for_health(args, timeout=60):
        _spark_rollout_restart(args)
        if not _spark_wait_for_health(args, timeout=180):
            raise RuntimeError("Spark master not Ready after rollout restart")
        _SPARK_CONSECUTIVE_TIMEOUTS["count"] = 0
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
    # M4: explicit app naming so we can `spark-submit --kill <id>` between cells
    # and free executors that would otherwise zombie on the master.
    app_name = (
        f"tfm-{algo.name}-{phase}-n{nodes}-e{total_executors}"
        f"-m{executor_memory}-run{run_index}"
    )
    env["SPARK_APP_NAME"] = app_name

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
    elif algo.name == "pagerank":
        env["PAGERANK_SPARK_SMOKE_NODES"] = str(nodes)
        env["PAGERANK_SPARK_SMOKE_PARTITIONS"] = str(partitions)
        env["PAGERANK_SPARK_SMOKE_MAX_ITER"] = str(args.max_iter)
        env["PAGERANK_SPARK_SMOKE_GRAPH_FILE"] = str(graph_file)
        env["PAGERANK_SPARK_SMOKE_DAMPING"] = str(getattr(args, "pagerank_damping", 0.85))
        env["PAGERANK_SPARK_SMOKE_TOLERANCE"] = str(getattr(args, "pagerank_tolerance", 1e-6))
    log_path = (
        campaign_root / "logs" / phase
        / f"spark_{algo.name}_n{nodes}_e{total_executors}_m{executor_memory}_run{run_index}.log"
    )
    snap_label = f"spark_{algo.name}_n{nodes}_e{total_executors}_m{executor_memory}_run{run_index}"
    capture_resource_snapshot(args, campaign_root, phase, f"{snap_label}_pre")

    try:
        completed = run_command(
            ["bash", str(algo.workdir / algo.spark_smoke_script)],
            cwd=ROOT,
            env=env,
            timeout=cell_timeout,
            log_path=log_path,
        )
        _SPARK_CONSECUTIVE_TIMEOUTS["count"] = 0
    except subprocess.TimeoutExpired as exc:
        # GraphX at n=10M (notably SSSP/PageRank) can hang well past any
        # reasonable budget. Surface as a normal failure so the caller's
        # try/except records the cell as failed and the sweep continues,
        # instead of aborting the whole algorithm.
        # M4: kill by app name (drops the zombie holding executors), then fall
        # back to pkill if --kill failed. If 2 timeouts in a row → rollout
        # restart the whole cluster.
        _spark_kill_app_by_name(args, app_name)
        _SPARK_CONSECUTIVE_TIMEOUTS["count"] += 1
        if _SPARK_CONSECUTIVE_TIMEOUTS["count"] >= 2:
            print("[spark] 2 consecutive timeouts → rollout restart")
            _spark_rollout_restart(args)
            _SPARK_CONSECUTIVE_TIMEOUTS["count"] = 0
        # M4 Nivel 2: when the timeout was an explicit per-n cap (extended
        # tier, e.g. n=5M 15-min cap) record status="timeout_15min" as a
        # measured outcome instead of raising → caller does not replace it
        # with a generic failed_run_record. For the default cap, keep the
        # raise so the legacy code path (sweep aborts to next rep) holds.
        if timeout_overridden:
            timeout_record = {
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
                "status": f"timeout_{cell_timeout}s",
                "timeout_sec": cell_timeout,
                "error": (
                    f"Spark {algo.name} cell hit per-n hard cap "
                    f"{cell_timeout}s (override). Recorded as measured timeout."
                ),
                "result": {},
                "phase_metrics": {},
            }
            write_json(raw_path, timeout_record)
            return timeout_record
        raise RuntimeError(
            f"Spark {algo.name} run timed out after {cell_timeout}s, see {log_path}"
        ) from exc
    finally:
        # M4: every cell ends with an explicit app kill so executors release
        # before the next cell starts (zombie-app fix, 0529 root cause). Safe
        # to call when the JVM already exited (no-op on the master).
        try:
            _spark_kill_app_by_name(args, app_name)
        except Exception as kill_exc:  # noqa: BLE001
            print(f"[spark] post-cell kill of {app_name} failed (ignored): {kill_exc}")
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
            raw_path = (
                campaign_root / "raw_runs" / "burst"
                / (
                    f"{algo.name}_characterization_n{args.characterization_nodes}"
                    f"_p{args.burst_partitions}_g{granularity}"
                    f"_m{args.characterization_memory_mb}_ck{args.chunk_size_kb}"
                    f"_run{run_index}.json"
                )
            )
            try:
                row = run_burst(
                    args, algo, campaign_root,
                    phase="characterization",
                    nodes=args.characterization_nodes,
                    partitions=args.burst_partitions,
                    granularity=granularity,
                    memory_mb=args.characterization_memory_mb,
                    run_index=run_index,
                    graph_file=graph_file,
                )
            except Exception as exc:
                log_path = (
                    campaign_root / "logs" / "characterization"
                    / (
                        f"burst_{algo.name}_characterization_n{args.characterization_nodes}"
                        f"_p{args.burst_partitions}_g{granularity}"
                        f"_m{args.characterization_memory_mb}_ck{args.chunk_size_kb}"
                        f"_run{run_index}.log"
                    )
                )
                row = failed_run_record(
                    phase="characterization", framework="burst", algorithm=algo.name,
                    nodes=args.characterization_nodes,
                    partitions=args.burst_partitions,
                    granularity=granularity,
                    memory_mb=args.characterization_memory_mb,
                    run_index=run_index,
                    graph_file=str(graph_file),
                    log_path=str(log_path),
                    error=str(exc),
                )
                write_json(raw_path, row)
            app_rows.append(row)
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


def _planned_burst_memory_mb(args: argparse.Namespace, algo: AlgorithmConfig, nodes: int) -> int:
    """Authoritative per-cell Burst worker memory (MiB): scaled by graph size n,
    clamped to the single-invoker user-memory budget. config_sweep contributes
    the *granularity* winner; memory is set here by a size-scaled safe bound so a
    flat config_sweep value can't under-provision a larger n and OOM the cell."""
    return burst_memory_mb(algo.name, nodes, budget_mb=args.burst_effective_user_memory_mb)


def _burst_fit_or_block(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    *,
    phase: str,
    nodes: int,
    partitions: int,
    granularity: int,
    memory_mb: int,
    run_index: int,
    graph_file: Path,
) -> dict[str, Any] | None:
    """Pre-launch fit-gate. Returns a ``status=blocked`` record (skip the cell)
    when the request can't fit a single worker node, else None (proceed). This
    turns a mid-run OOM into a fast, explicit, defensible skip."""
    ok, request = burst_cell_fit(partitions=partitions, granularity=granularity, memory_mb=memory_mb)
    if ok:
        return None
    budget = cloudlab_node_budget()
    reason = (
        f"infeasible on single node: {request.memory_mb} MiB requested "
        f"(p={partitions} × {memory_mb} MiB + {budget.reserved_memory_mb} reserved) "
        f"> usable {budget.usable_memory_mb} MiB. Reduce partitions or memory."
    )
    print(
        f"[fit] BLOCKED burst {algo.name} n={nodes} p={partitions} g={granularity} "
        f"m={memory_mb}: {reason}"
    )
    return failed_run_record(
        status="blocked", phase=phase, framework="burst", algorithm=algo.name,
        nodes=nodes, partitions=partitions, granularity=granularity, memory_mb=memory_mb,
        run_index=run_index, graph_file=str(graph_file), error=reason,
    )


def _spark_fit_or_block(
    algo: AlgorithmConfig,
    *,
    phase: str,
    nodes: int,
    partitions: int,
    executors: int,
    executor_memory: str,
    run_index: int,
    graph_file: Path,
) -> dict[str, Any] | None:
    """Pre-launch fit-gate for Spark cells (single-node budget)."""
    ok, request = spark_cell_fit(executors=executors, executor_memory=executor_memory)
    if ok:
        return None
    budget = cloudlab_node_budget()
    reason = (
        f"infeasible on single node: {request.memory_mb} MiB requested "
        f"({executors} × {executor_memory} + master) > usable {budget.usable_memory_mb} MiB."
    )
    print(
        f"[fit] BLOCKED spark {algo.name} n={nodes} ex={executors} mem={executor_memory}: {reason}"
    )
    return failed_run_record(
        status="blocked", phase=phase, framework="spark", algorithm=algo.name,
        nodes=nodes, partitions=partitions, executors=executors,
        executor_memory=executor_memory, run_index=run_index,
        graph_file=str(graph_file), error=reason,
    )


def _dry_run_plan(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    burst_enabled: bool,
    spark_enabled: bool,
    cost_enabled: bool,
) -> None:
    """Print the planned matrix with per-cell resource requests + fit results,
    without executing or touching the cluster. Surfaces infeasible cells (which
    would OOM mid-run) before any time is spent."""
    budget = cloudlab_node_budget()
    print("=" * 70)
    print(f"DRY RUN — {algo.display_name}  (node budget: "
          f"{budget.usable_memory_mb} MiB / {budget.usable_cpus} cpu usable)")
    print("=" * 70)
    infeasible = 0

    if burst_enabled:
        print("\n[burst size_sweep]")
        for bp in args.burst_partition_list:
            cfg = _read_best_config(args.campaign_root, algo.name, bp) or {}
            gran = int((cfg.get("burst") or {}).get("granularity", 0) or 0)
            for n in args.size_nodes:
                mem = _planned_burst_memory_mb(args, algo, n)
                ok, req = burst_cell_fit(partitions=bp, granularity=gran or 2, memory_mb=mem)
                flag = "OK   " if ok else "BLOCK"
                infeasible += 0 if ok else 1
                print(f"  [{flag}] n={n:>9} p={bp:>2} g={gran or '?'} m={mem:>5} "
                      f"-> req {req.memory_mb:>6} MiB / {req.cpus:g} cpu")

    if spark_enabled:
        print("\n[spark size_sweep]")
        spark_sizes = getattr(args, "spark_size_node_list", None) or args.size_nodes
        override = getattr(args, "spark_executor_memory_override_map", {}) or {}
        for bp in args.burst_partition_list:
            execs = args.spark_partition_executor_map.get(bp, args.spark_executor_list[0])
            for n in spark_sizes:
                mem = str(override.get(n, args.spark_config_memories[0]))
                ok, req = spark_cell_fit(executors=execs, executor_memory=mem)
                flag = "OK   " if ok else "BLOCK"
                infeasible += 0 if ok else 1
                print(f"  [{flag}] n={n:>9} ex={execs:>2} mem={mem:>4} "
                      f"-> req {req.memory_mb:>6} MiB / {req.cpus:g} cpu")

    if cost_enabled:
        cost_backends = [b for b in args.backend_list if b in ("standalone", "rayon", "mpi")]
        n_cells = len(args.cost_sweep_node_list) * args.cost_runs
        print(f"\n[cost_sweep] backends={cost_backends} "
              f"nodes={args.cost_sweep_node_list} (~{n_cells} cells/backend, host-local)")

    print("\n" + "-" * 70)
    if infeasible:
        print(f"DRY RUN: {infeasible} INFEASIBLE cell(s) would be blocked. "
              f"Reduce partitions/memory or they will be skipped.")
    else:
        print("DRY RUN: all burst/spark cells fit the node budget.")
    print("-" * 70)


def _incomplete_rows(rows: Any) -> list[dict[str, Any]]:
    """Rows that are neither 'passed'/'ok' nor a measured timeout finding."""
    out: list[dict[str, Any]] = []
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        status = r.get("status")
        if status in ("passed", "ok"):
            continue
        if isinstance(status, str) and status.startswith("timeout"):
            continue  # measured structural finding, intentionally kept
        out.append(r)
    return out


def report_campaign_health(args: argparse.Namespace, algo: AlgorithmConfig, campaign_root: Path) -> int:
    """Scan the aggregates for cells that did not pass and print them plus a
    ready-to-paste resume command. Returns the count of incomplete cells. This
    replaces the bespoke resume_*.sh scripts: the operator sees exactly what to
    re-run and how."""
    incomplete: list[tuple[str, dict[str, Any]]] = []
    size_dir = campaign_root / "size_sweep"
    for pat in (f"{algo.name}_burst_runs_p*.json", f"{algo.name}_spark_runs_p*.json"):
        for path in sorted(size_dir.glob(pat)):
            for row in _incomplete_rows(read_cached_record_list(path)):
                incomplete.append((path.name, row))
    cost_runs_dir = campaign_root / "cost_sweep"
    for path in sorted(cost_runs_dir.glob("runs_*.json")):
        for row in _incomplete_rows(read_cached_record_list(path)):
            incomplete.append((path.name, row))

    print(f"\n{'-'*60}\n[health] {algo.display_name}: ", end="")
    if not incomplete:
        print("all cells passed (or recorded as measured timeouts).")
        return 0
    print(f"{len(incomplete)} incomplete cell(s):")
    for src, row in incomplete[:40]:
        ident = (
            f"n={row.get('nodes')} p={row.get('partitions')} "
            f"g={row.get('granularity', '-')} m={row.get('memory_mb', row.get('executor_memory', '-'))} "
            f"backend={row.get('framework')} status={row.get('status')}"
        )
        err = (row.get("error") or "")[:80]
        print(f"  [{src}] {ident}  {err}")
    print(
        "\n[health] resume with (cached passes are skipped, failures re-run):\n"
        f"    PHASE=full ALGORITHMS={shlex.quote(algo.name)} "
        f"CAMPAIGN_ROOT={shlex.quote(str(campaign_root))} bash campaigns/launch_campaign_v3.sh\n"
        f"  or directly:\n"
        f"    python3 campaigns/run_cloudlab_campaign.py --algorithm {algo.name} "
        f"--resume --campaign-root {shlex.quote(str(campaign_root))} [matrix args]"
    )
    return len(incomplete)


def read_cached_record_list(path: Path) -> list[dict[str, Any]]:
    """Read a JSON array of records, tolerating a corrupt/missing file."""
    try:
        data = read_json(path)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def run_size_sweep(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
    winners: dict[str, Any],
) -> dict[str, Any]:
    burst_rows: list[dict[str, Any]] = []
    spark_rows: list[dict[str, Any]] = []
    has_burst_winner = "burst" in winners and winners["burst"] and "burst" in args.backend_list
    has_spark_winner = "spark" in winners and winners["spark"] and "spark" in args.backend_list

    for nodes in args.size_nodes:
        graph_file = ensure_local_graph_file(args, algo, campaign_root, nodes=nodes, partitions=args.burst_partitions)
        burst_granularity = int(winners["burst"]["granularity"]) if has_burst_winner else 0
        burst_memory = _planned_burst_memory_mb(args, algo, nodes) if has_burst_winner else 0
        for run_index in range(1, args.size_runs + 1):
            if has_burst_winner:
                blocked = _burst_fit_or_block(
                    args, algo, phase="size_sweep", nodes=nodes,
                    partitions=args.burst_partitions, granularity=burst_granularity,
                    memory_mb=burst_memory, run_index=run_index, graph_file=graph_file,
                )
                if blocked is not None:
                    burst_rows.append(blocked)
                else:
                    try:
                        burst_row = run_burst(
                            args, algo, campaign_root,
                            phase="size_sweep",
                            nodes=nodes,
                            partitions=args.burst_partitions,
                            granularity=burst_granularity,
                            memory_mb=burst_memory,
                            run_index=run_index,
                            graph_file=graph_file,
                        )
                    except Exception as exc:
                        burst_row = failed_run_record(
                            phase="size_sweep", framework="burst", algorithm=algo.name,
                            nodes=nodes, partitions=args.burst_partitions,
                            granularity=burst_granularity,
                            memory_mb=burst_memory,
                            run_index=run_index, graph_file=str(graph_file),
                            error=str(exc),
                        )
                    burst_rows.append(burst_row)

            # Spark may be capped to a subset of sizes (--spark-size-nodes):
            # GraphX at n=10M costs ~2h/cell and only confirms the order-of-
            # magnitude loss already visible at n<=1M, so it is skipped by
            # default to avoid burning ~18h on redundant, timeout-prone cells.
            spark_sizes = getattr(args, "spark_size_node_list", None) or args.size_nodes
            if has_spark_winner and nodes in spark_sizes:
                spark_executors = int(winners["spark"]["executors"])
                # Mirror run_spark's per-n executor-memory override so the
                # fit-gate checks the value the cell will actually request.
                _exec_override = getattr(args, "spark_executor_memory_override_map", {}) or {}
                spark_exec_mem = str(_exec_override.get(nodes, winners["spark"]["executor_memory"]))
                blocked = _spark_fit_or_block(
                    algo, phase="size_sweep", nodes=nodes, partitions=args.spark_partitions,
                    executors=spark_executors, executor_memory=spark_exec_mem,
                    run_index=run_index, graph_file=graph_file,
                )
                if blocked is not None:
                    spark_rows.append(blocked)
                    continue
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
    burst_path = campaign_root / "size_sweep" / f"{algo.name}_burst_runs_p{p}.json"
    spark_path = campaign_root / "size_sweep" / f"{algo.name}_spark_runs_p{p}.json"
    summary_path = campaign_root / "size_sweep" / f"{algo.name}_summary_p{p}.json"

    # Don't wipe an existing aggregate when this sweep didn't run the
    # corresponding backend (e.g. --backends spark on a campaign that
    # already has cached burst rows from a previous pass).
    if burst_rows:
        write_json(burst_path, burst_rows)
    elif not has_burst_winner and burst_path.exists():
        burst_rows = read_json(burst_path) or []
    if spark_rows:
        write_json(spark_path, spark_rows)
    elif not has_spark_winner and spark_path.exists():
        spark_rows = read_json(spark_path) or []

    # Recompute the summary block to reflect whatever ended up in
    # burst_rows / spark_rows (fresh + reused).
    burst_passed_final = [r for r in burst_rows if r.get("status") in ("ok", "passed")]
    spark_passed_final = [r for r in spark_rows if r.get("status") in ("ok", "passed")]
    burst_summary_final = summarize_rows(burst_passed_final, metric_extractor=burst_metric) if burst_passed_final else []
    spark_summary_final = summarize_rows(spark_passed_final, metric_extractor=spark_metric) if spark_passed_final else []
    payload = {"burst": burst_summary_final, "spark": spark_summary_final}
    write_json(summary_path, payload)
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

def _validate_remote_cost_binaries(
    args: argparse.Namespace,
    cfg: Any,
    backends: list[str],
) -> None:
    """Probe via SSH that the configured COST binaries exist on the target host.

    Fails fast with a clear error instead of letting individual cells fail
    deep inside ``run_standalone_remote`` / ``run_rayon_remote`` / ``run_mpi_remote``.
    """
    remote_dir = f"{args.cloudlab_src_root}/{cfg.workdir_name}"
    checks = []
    if "standalone" in backends:
        checks.append(("standalone", cfg.standalone_binary))
    if "rayon" in backends:
        checks.append(("rayon", cfg.rayon_binary))
    if "mpi" in backends:
        checks.append(("mpi", cfg.mpi_binary))
    missing = []
    for label, path in checks:
        cmd = f"test -x {shlex.quote(f'{remote_dir}/{path}')} && echo OK || echo MISSING"
        result = ssh_command(args, cmd, timeout=60)
        if "MISSING" in result.stdout or result.returncode != 0:
            missing.append(f"{label}: {remote_dir}/{path}")
    if missing:
        raise RuntimeError(
            "[cost_sweep] required Rust binaries not found on CloudLab host. "
            "Re-run the compile scripts (compile_<algo>_cost_backends.sh / "
            "compile_<algo>_cluster.sh) on compute6/7 first. Missing:\n  "
            + "\n  ".join(missing)
        )


def _cost_cell_id(algo: AlgorithmConfig, cell: dict[str, Any]) -> str:
    """Stable identifier for a single COST cell, used as raw_runs filename."""
    backend = cell["backend"]
    nodes = cell["nodes"]
    rep = cell["rep"]
    if backend == "rayon":
        variant = f"t{cell['threads']}"
    elif backend == "mpi":
        variant = f"r{cell['ranks']}"
    else:
        variant = "single"
    return f"cost_{algo.name}_{backend}_n{nodes}_{variant}_run{rep}"


def run_cost_sweep(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
) -> None:
    """Drive the standalone/rayon/mpi COST extension over the configured matrix.

    Per cell: upload graph file to CloudLab once (idempotent), execute via SSH,
    capture stdout JSON, write to raw_runs/cost-{backend}/<cell_id>.json.
    Aggregates into cost_sweep/runs_{backend}.json + summary.json.
    """
    if algo.name not in COST_BACKEND_CONFIGS:
        print(f"[cost_sweep] no COST backend config for {algo.name}; skipping.")
        return
    cfg = COST_BACKEND_CONFIGS[algo.name]
    cost_backends = [b for b in args.backend_list if b in {"standalone", "rayon", "mpi"}]
    if not cost_backends:
        print("[cost_sweep] no COST backends enabled; skipping.")
        return

    out_dir = campaign_root / "cost_sweep"
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir_base = campaign_root / "raw_runs"
    for backend in cost_backends:
        (raw_dir_base / f"cost-{backend}").mkdir(parents=True, exist_ok=True)
    (campaign_root / "logs" / "cost_sweep").mkdir(parents=True, exist_ok=True)

    remote_dataset_root = f"/tmp/{campaign_root.name}/datasets"

    # Cross-host MPI needs the dataset + binary on every compute node, since
    # /home is not shared (ext2/3, not NFS). Detect extra hosts once.
    mpi_enabled = "mpi" in cost_backends
    extra_hosts = mpi_extra_hosts(args) if mpi_enabled else []
    if extra_hosts:
        print(f"[cost_sweep] cross-host MPI: will propagate dataset+binary to {extra_hosts}")

    # Stage graph files (single-file TSV) on CloudLab, one per n.
    remote_graphs: dict[int, str] = {}
    for n in args.cost_sweep_node_list:
        local_graph = ensure_local_graph_file(
            args, algo, campaign_root, nodes=n, partitions=args.burst_partition_list[0],
        )
        remote_graphs[n] = ensure_remote_graph_file(
            args, cfg, n, local_graph, remote_dataset_root,
        )
        if extra_hosts:
            propagate_remote_file(args, remote_graphs[n])

    # Propagate the MPI binary to the extra hosts (same absolute path). The
    # OpenMPI runtime is already installed per-node at --mpi-prefix; only the
    # algorithm binary is missing on the peers.
    if extra_hosts:
        mpi_binary_path = f"{args.cloudlab_src_root}/{cfg.workdir_name}/{cfg.mpi_binary}"
        propagate_remote_file(args, mpi_binary_path)

    cells = expand_cost_cells(
        backends=cost_backends,
        nodes_list=args.cost_sweep_node_list,
        reps=args.cost_runs,
        rayon_threads=args.rayon_thread_list,
        mpi_ranks=args.mpi_rank_list,
        reps_overrides=getattr(args, "cost_runs_override_map", None),
    )

    rows_by_backend: dict[str, list[dict[str, Any]]] = {b: [] for b in cost_backends}

    # Optional binary probe — fail fast if compile missed a target.
    if getattr(args, "validate_binaries", False):
        _validate_remote_cost_binaries(args, cfg, cost_backends)

    def _compact_cost_record(record: dict[str, Any]) -> dict[str, Any]:
        """Keep aggregate rows report-sized; raw_runs retain full payloads."""
        result = record.get("result") if isinstance(record.get("result"), dict) else {}
        raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
        compact_raw: dict[str, Any] = {}
        for key in ("execution_time_ms", "compute_ms", "communication_ms"):
            value = raw.get(key)
            if isinstance(value, (int, float)):
                compact_raw[key] = value
        compact = {
            "phase": record.get("phase", "cost_sweep"),
            "framework": record.get("framework"),
            "algorithm": record.get("algorithm"),
            "backend": record.get("backend"),
            "nodes": record.get("nodes"),
            "rep": record.get("rep"),
            "status": record.get("status", result.get("status", "failed")),
            "result": {"raw": compact_raw},
        }
        for key in ("threads", "ranks"):
            value = record.get(key, raw.get(key))
            if value is not None:
                compact[key] = value
                compact_raw.setdefault(key, value)
        return compact

    def _timeout_for(backend: str, n: int) -> int:
        override = {
            "standalone": getattr(args, "cost_timeout_standalone_sec", None),
            "rayon": getattr(args, "cost_timeout_rayon_sec", None),
            "mpi": getattr(args, "cost_timeout_mpi_sec", None),
        }.get(backend)
        if override is not None:
            return override
        base = args.cost_cell_timeout_sec
        # Standalone scales linearly with n; n>=5M typically 5-10× the default.
        if backend == "standalone" and n >= 5_000_000:
            return base * 3
        return base

    for cell in cells:
        backend = cell["backend"]
        n = cell["nodes"]
        cell_id = _cost_cell_id(algo, cell)
        raw_path = raw_dir_base / f"cost-{backend}" / f"{cell_id}.json"
        if raw_path.exists():
            cached = read_cached_record(raw_path)
            if cached is not None and cached_record_ok(cached):
                rows_by_backend[backend].append(_compact_cost_record(cached))
                print(f"[cost_sweep] cached {cell_id}")
                continue
            print(f"[cost_sweep] cache invalid for {cell_id}; regenerating")

        remote_graph = remote_graphs[n]
        snap_label = f"cost_{algo.name}_{cell_id}"
        capture_resource_snapshot(args, campaign_root, "cost_sweep", f"{snap_label}_pre")
        timeout = _timeout_for(backend, n)
        if backend == "standalone":
            result = run_standalone_remote(
                args, cfg, remote_graph, n, timeout,
            )
        elif backend == "rayon":
            result = run_rayon_remote(
                args, cfg, remote_graph, n, cell["threads"], timeout,
            )
        elif backend == "mpi":
            result = run_mpi_remote(
                args, cfg, remote_graph, n, cell["ranks"], args.mpi_hosts, timeout,
            )
        else:
            continue
        capture_resource_snapshot(args, campaign_root, "cost_sweep", f"{snap_label}_post")

        record = {
            "phase": "cost_sweep",
            "framework": f"cost-{backend}",
            "algorithm": algo.name,
            "backend": backend,
            "nodes": n,
            "rep": cell["rep"],
        }
        for k in ("threads", "ranks"):
            if k in cell:
                record[k] = cell[k]
        record["result"] = result
        record["status"] = result.get("status", "failed")
        write_json(raw_path, record)
        rows_by_backend[backend].append(_compact_cost_record(record))
        print(f"[cost_sweep] {cell_id} → {record['status']}")

    # Write compact aggregate files for the current algorithm. Full benchmark
    # payloads stay in raw_runs/cost-*; copying them into runs_*.json made
    # reruns materialize tens of GB in memory for 10M-node MPI cells.
    for backend in rows_by_backend:
        agg_path = out_dir / f"runs_{backend}.json"
        existing = []
        if agg_path.exists():
            # Old campaigns may have pre-fix aggregate files in the multi-GB
            # range. Do not read them; this pass rewrites the touched rows in
            # compact form. New compact aggregates stay small and can be merged.
            if agg_path.stat().st_size < 100 * 1024 * 1024:
                existing = read_json(agg_path)
                if not isinstance(existing, list):
                    existing = []
            else:
                print(f"[cost_sweep] ignoring legacy oversized aggregate {agg_path}")
        touched = {
            (r.get("algorithm"), r.get("nodes"), r.get("rep"), r.get("threads"), r.get("ranks"))
            for r in rows_by_backend[backend]
        }
        kept = [
            r for r in existing
            if (r.get("algorithm"), r.get("nodes"), r.get("rep"), r.get("threads"), r.get("ranks")) not in touched
        ]
        rows_by_backend[backend] = kept + rows_by_backend[backend]
        write_json(agg_path, rows_by_backend[backend])

    summary: dict[str, Any] = {}
    for backend, rows in rows_by_backend.items():
        by_key: dict[str, list[float]] = {}
        for r in rows:
            if r.get("status") != "passed":
                continue
            raw = (r.get("result") or {}).get("raw") or {}
            t = raw.get("execution_time_ms")
            if not isinstance(t, (int, float)) or t <= 0:
                continue
            variant_key = str(r.get("threads", r.get("ranks", "single")))
            key = f"n{r['nodes']}_{backend}{variant_key}"
            by_key.setdefault(key, []).append(float(t))
        summary[backend] = {
            k: {
                "n": len(v),
                "median_ms": sorted(v)[len(v) // 2],
                "min_ms": min(v),
                "max_ms": max(v),
            }
            for k, v in by_key.items()
        }
    write_json(out_dir / "summary.json", summary)


def run_report_phase(
    args: argparse.Namespace,
    algo: AlgorithmConfig,
    campaign_root: Path,
) -> None:
    """Auto-generate markdown tables + PNG figures from cost_sweep + size_sweep
    JSONs. Idempotent: overwrites prior report/ outputs.
    """
    try:
        from report_generators import (
            render_campaign_summary,
            render_cost_report,
            render_cross_backend_table,
            render_size_figures,
            render_warmpool_breakdown,
        )
    except ImportError as exc:
        print(f"[report] report_generators unavailable ({exc}); skipping.")
        return
    report_dir = campaign_root / "report" / algo.name
    report_dir.mkdir(parents=True, exist_ok=True)
    render_cost_report(campaign_root, algo.name, report_dir)
    render_size_figures(campaign_root, algo.name, report_dir)
    render_cross_backend_table(campaign_root, algo.name, report_dir)
    render_warmpool_breakdown(campaign_root, algo.name, report_dir)
    render_campaign_summary(campaign_root, algo.name)
    print(f"[report] wrote {report_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a CloudLab campaign for a graph algorithm.")
    parser.add_argument("--algorithm", choices=list(ALGORITHMS), required=True)
    parser.add_argument("--campaign-root", type=Path, default=None)
    parser.add_argument(
        "--phase",
        choices=["full", "preflight", "characterization", "config", "chunk_probe", "size", "cost", "report"],
        default="full",
    )
    parser.add_argument(
        "--backends",
        default="standalone,rayon,mpi,burst,spark",
        help="Comma-separated backends to run. standalone/rayon/mpi feed cost_sweep; burst/spark feed size_sweep.",
    )
    parser.add_argument("--cloudlab-user", default="sconde")
    parser.add_argument("--cloudlab-host", default="cloudfunctions.urv.cat")
    parser.add_argument("--cloudlab-ssh-key", default="/home/sergio/.ssh/id_pc1")
    parser.add_argument(
        "--cloudlab-ssh-config",
        default=os.environ.get("CLOUDLAB_SSH_CONFIG"),
        help="Optional SSH config file passed as `ssh -F`/`scp -F` for ProxyJump aliases.",
    )
    parser.add_argument("--cloudlab-src-root", default=REMOTE_DEFAULT_SRC_ROOT)
    parser.add_argument("--ow-namespace", default="openwhisk")
    parser.add_argument("--ow-release-name", default="owdev")
    parser.add_argument("--ow-host", default="127.0.0.1")
    parser.add_argument("--ow-port", type=int, default=31002)
    parser.add_argument("--ow-protocol", choices=["http", "https"], default="http")
    parser.add_argument("--spark-namespace", default="spark-sconde-smoke")
    parser.add_argument(
        "--spark-kubectl-host",
        default="cloudfunctions.urv.cat",
        help="Host with a working kubeconfig (proxy/control-plane) used for "
             "best-effort spark-shell cleanup after a Spark cell timeout.",
    )
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
    parser.add_argument(
        "--burst-warmup-shots",
        type=int,
        default=9,
        help=(
            "Warm-pool protocol: 1 cold (discarded) + N warm reps per Burst cell. "
            "0 = legacy single-shot cold. Default 9 → measures serverless steady-state."
        ),
    )
    parser.add_argument(
        "--burst-warmup-shots-overrides",
        default="",
        help=(
            "Per-n override map for warm-pool shots, format 'n:shots[,n:shots,...]'. "
            "Example '10000000:5' caps n=10M Burst cells to 5 warm reps "
            "(eviction risk: 9 shots at n=10M for SSSP/PR can exceed OW idleTimeout)."
        ),
    )
    parser.add_argument("--burst-partitions", default="4,8,16")
    parser.add_argument("--burst-effective-invokers", type=int, default=2)
    parser.add_argument("--burst-effective-user-memory-mb", type=int, default=8192)
    parser.add_argument("--spark-partitions", default="4,8,16")
    parser.add_argument("--spark-total-executors", default="4,8,16")
    parser.add_argument("--spark-config-memories", default="4g,6g")
    parser.add_argument("--size-nodes", default="100000,500000,1000000,2000000")
    parser.add_argument(
        "--spark-size-nodes",
        default=None,
        help=(
            "Optional comma list capping which sizes Spark runs in size_sweep "
            "(subset of --size-nodes). Empty/unset => all sizes. Use to skip "
            "Spark at n=10M, where GraphX costs ~2h/cell and only re-confirms "
            "the order-of-magnitude loss already visible at n<=1M."
        ),
    )
    parser.add_argument("--chunk-probe-sizes", default="64,256,1024,4096")
    parser.add_argument(
        "--cost-sweep-nodes",
        default="10000,50000,100000,500000,1000000,2000000,5000000,10000000",
        help="Node counts for COST sweep (standalone/rayon/mpi single-file matrix).",
    )
    parser.add_argument("--cost-runs", type=int, default=3, help="Repetitions per COST cell.")
    parser.add_argument(
        "--cost-runs-overrides",
        default="",
        help=(
            "Per-n override map for COST repetitions, format 'n:reps[,n:reps,...]'. "
            "Example '10000000:5' bumps n=10M cells to 5 reps. Applies only to "
            "standalone/rayon/mpi (Burst uses warm-pool single-record)."
        ),
    )
    parser.add_argument(
        "--rayon-threads",
        default="1,2,4,8,16,32",
        help="Comma-separated RAYON_NUM_THREADS values for rayon sweep.",
    )
    parser.add_argument(
        "--mpi-ranks",
        default="4,8,16,32",
        help="Comma-separated mpirun -np values for MPI sweep.",
    )
    parser.add_argument(
        "--mpi-hosts",
        default="compute6,compute7",
        help="mpirun -H hostlist for MPI sweep on CloudLab.",
    )
    parser.add_argument(
        "--mpi-map-by",
        default=None,
        help="Optional mpirun --map-by policy. Use 'node' to force cross-host placement.",
    )
    parser.add_argument(
        "--mpi-prefix",
        default=None,
        help="--prefix path for remote orted (e.g. /home/users/sconde/opt/openmpi-4.1.5).",
    )
    parser.add_argument(
        "--mpi-btl-if-include",
        default=None,
        help="Restrict OOB+BTL TCP to subnet (e.g. 192.168.5.0/24).",
    )
    parser.add_argument(
        "--cost-cell-timeout-sec",
        type=int,
        default=1800,
        help="Default per-cell timeout for COST backends (standalone/rayon/mpi).",
    )
    parser.add_argument(
        "--cost-timeout-standalone-sec",
        type=int,
        default=None,
        help="Override --cost-cell-timeout-sec for standalone cells. Defaults to 3× cell timeout for n>=5M.",
    )
    parser.add_argument(
        "--cost-timeout-rayon-sec",
        type=int,
        default=None,
        help="Override --cost-cell-timeout-sec for Rayon cells.",
    )
    parser.add_argument(
        "--cost-timeout-mpi-sec",
        type=int,
        default=None,
        help="Override --cost-cell-timeout-sec for MPI cells (cross-host comms can be slow).",
    )
    parser.add_argument(
        "--spark-cell-timeout-sec",
        type=int,
        default=5400,
        help=(
            "Per-cell wall-clock timeout for a Spark run. On timeout the cell is "
            "recorded as failed and the sweep continues (matters with size-runs>1, "
            "where slow GraphX cells at n=10M would otherwise burn 2h each, per rep)."
        ),
    )
    parser.add_argument(
        "--spark-size-timeout-overrides",
        default="",
        help=(
            "Per-n override map for Spark cell timeout, format 'n:seconds[,...]'. "
            "Example '5000000:900' caps n=5M at 15 min so the extended-tier cell "
            "either converges or records status=timeout_15min as a measured finding."
        ),
    )
    parser.add_argument(
        "--spark-executor-memory-overrides",
        default="",
        help=(
            "Per-n override map for Spark executor memory, format 'n:mem[,...]'. "
            "Example '5000000:12g' bumps n=5M executors to 12 GiB to give GraphX "
            "the headroom needed before timing out. Mem string passed verbatim "
            "to SPARK_EXECUTOR_MEMORY (e.g. '4g', '12g')."
        ),
    )
    parser.add_argument(
        "--validate-binaries",
        action="store_true",
        help=(
            "Probe required Rust binaries via SSH before starting cost_sweep. "
            "Aborts the run if any expected binary is missing on CloudLab compute nodes."
        ),
    )
    parser.add_argument(
        "--external-graph-tsv",
        default=None,
        help=(
            "Path to a real-world TSV graph file (e.g. SNAP soc-LiveJournal1). "
            "When set together with --external-graph-num-nodes, the orchestrator "
            "uses this file instead of the synthetic generator for cells whose "
            "node count matches. Recommended: download "
            "https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz "
            "(4,847,571 nodes, 68,993,773 edges) and pass "
            "--external-graph-num-nodes 4847571."
        ),
    )
    parser.add_argument(
        "--external-graph-num-nodes",
        type=int,
        default=None,
        help="Node count of the external graph (must match the file's vertex range).",
    )
    parser.add_argument("--spark-master-request-cpu", default="1")
    parser.add_argument("--spark-master-limit-cpu", default="2")
    parser.add_argument("--spark-master-request-memory", default="4Gi")
    parser.add_argument("--spark-master-limit-memory", default="8Gi")
    # -- Resilience: preflight gate, dry-run, resume -----------------------
    parser.add_argument(
        "--skip-preflight", action="store_true",
        help="Skip the cluster health gate (NOT recommended; for offline/report-only runs).",
    )
    parser.add_argument(
        "--preflight-detect-only", action="store_true",
        help="Run the cluster health gate in detect-and-report mode (no auto-remediation).",
    )
    parser.add_argument(
        "--preflight-ready-timeout-sec", type=int, default=300,
        help="Seconds to wait for core pods to become Ready after remediation.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Plan the campaign: print every cell with its resource request and "
             "fit-check result, then exit without executing or touching the cluster.",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Re-scan for missing/failed cells across all phases and run only those "
             "(cached passed cells are skipped). Replaces the ad-hoc resume_*.sh scripts.",
    )
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
    # Optional Spark size subset; empty/unset => same as size_nodes.
    if getattr(args, "spark_size_nodes", None):
        args.spark_size_node_list = [int(t.strip()) for t in args.spark_size_nodes.split(",") if t.strip()]
    else:
        args.spark_size_node_list = list(args.size_nodes)
    args.chunk_probe_size_list = [int(t.strip()) for t in args.chunk_probe_sizes.split(",") if t.strip()]
    args.backend_list = [b.strip() for b in args.backends.split(",") if b.strip()]
    args.cost_sweep_node_list = [int(t.strip()) for t in args.cost_sweep_nodes.split(",") if t.strip()]
    args.rayon_thread_list = [int(t.strip()) for t in args.rayon_threads.split(",") if t.strip()]
    args.mpi_rank_list = [int(t.strip()) for t in args.mpi_ranks.split(",") if t.strip()]
    args.cost_runs_override_map: dict[int, int] = {}
    if getattr(args, "cost_runs_overrides", ""):
        for token in args.cost_runs_overrides.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                n_str, reps_str = token.split(":", 1)
                args.cost_runs_override_map[int(n_str)] = int(reps_str)
            except ValueError:
                raise SystemExit(f"invalid --cost-runs-overrides token: {token!r} (expected 'n:reps')")
    args.burst_warmup_shots_override_map: dict[int, int] = {}
    if getattr(args, "burst_warmup_shots_overrides", ""):
        for token in args.burst_warmup_shots_overrides.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                n_str, shots_str = token.split(":", 1)
                args.burst_warmup_shots_override_map[int(n_str)] = int(shots_str)
            except ValueError:
                raise SystemExit(
                    f"invalid --burst-warmup-shots-overrides token: {token!r} (expected 'n:shots')"
                )
    args.spark_size_timeout_override_map: dict[int, int] = {}
    if getattr(args, "spark_size_timeout_overrides", ""):
        for token in args.spark_size_timeout_overrides.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                n_str, sec_str = token.split(":", 1)
                args.spark_size_timeout_override_map[int(n_str)] = int(sec_str)
            except ValueError:
                raise SystemExit(
                    f"invalid --spark-size-timeout-overrides token: {token!r} (expected 'n:seconds')"
                )
    args.spark_executor_memory_override_map: dict[int, str] = {}
    if getattr(args, "spark_executor_memory_overrides", ""):
        for token in args.spark_executor_memory_overrides.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                n_str, mem_str = token.split(":", 1)
                args.spark_executor_memory_override_map[int(n_str)] = mem_str.strip()
            except ValueError:
                raise SystemExit(
                    f"invalid --spark-executor-memory-overrides token: {token!r} (expected 'n:mem')"
                )
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


_LEGACY_BEST_CONFIG_WARNED: set[str] = set()


def _best_config_path(campaign_root: Path, algo_name: str, bp: int) -> Path:
    return campaign_root / "config_sweep" / f"best_config_{algo_name}_p{bp}.json"


def _legacy_best_config_path(campaign_root: Path, bp: int) -> Path:
    return campaign_root / "config_sweep" / f"best_config_p{bp}.json"


def _read_best_config(campaign_root: Path, algo_name: str, bp: int) -> dict[str, Any] | None:
    """Read best_config for algo+partition. Prefer per-algo path, fall back to
    legacy global path for backward compatibility (logged once per algo)."""
    primary = _best_config_path(campaign_root, algo_name, bp)
    if primary.exists():
        return read_json(primary)
    legacy = _legacy_best_config_path(campaign_root, bp)
    if legacy.exists():
        key = f"{campaign_root}:{algo_name}"
        if key not in _LEGACY_BEST_CONFIG_WARNED:
            print(
                f"[warn] using legacy best_config path {legacy.name} for algo={algo_name}; "
                f"this file is shared across algos and can be wrong if another algo "
                f"overwrote it. Re-run --phase config to namespace it.",
                file=sys.stderr,
            )
            _LEGACY_BEST_CONFIG_WARNED.add(key)
        return read_json(legacy)
    return None


def _load_all_winners(args: argparse.Namespace, algo_name: str) -> dict[int, dict[str, Any]]:
    all_winners: dict[int, dict[str, Any]] = {}
    for bp in args.burst_partition_list:
        cfg = _read_best_config(args.campaign_root, algo_name, bp)
        if cfg is not None:
            all_winners[bp] = cfg
    return all_winners


_AUTHORITATIVE_PHASES = {"full", "preflight"}
_EPHEMERAL_METADATA_FIELDS = {"last_phase", "last_phase_run_at"}


def _persist_metadata(path: Path, fresh: dict[str, Any], phase: str) -> None:
    """Phase-aware metadata write.

    Authoritative phases (full, preflight) overwrite metadata.json with `fresh`.
    Other phases (config, chunk_probe, size, cost, report, characterization)
    preserve campaign-level fields if metadata.json already exists; they only
    bump ephemeral fields (last_phase, last_phase_run_at).

    Rationale: partial `--phase` reruns without re-supplying the full matrix
    (mpi_ranks, rayon_threads, ...) used to overwrite the matrix with parser
    defaults. Source of truth is `report/*` tables + raw_runs; metadata.json is
    informative and must not lie.
    """
    now = datetime.now(timezone.utc).isoformat()
    if phase in _AUTHORITATIVE_PHASES or not path.exists():
        merged = dict(fresh)
        merged["last_phase"] = phase
        merged["last_phase_run_at"] = now
        write_json(path, merged)
        return
    existing = read_json(path)
    if not isinstance(existing, dict):
        write_json(path, {**fresh, "last_phase": phase, "last_phase_run_at": now})
        return
    merged = dict(existing)
    merged["last_phase"] = phase
    merged["last_phase_run_at"] = now
    write_json(path, merged)


# ---------------------------------------------------------------------------
# Orphan cleanup + signal handling
# ---------------------------------------------------------------------------

_CLEANUP_DONE = {"ran": False}


def cleanup_all(args: argparse.Namespace) -> None:
    """Tear down every process/resource a campaign run can leave behind:
    the remote kubectl port-forward, Spark apps/JVMs, MPI orted/mpirun daemons
    on every host, and leftover guest/prewarm action pods. Idempotent and
    exception-safe — cleanup must never itself crash the run."""
    if _CLEANUP_DONE["ran"]:
        return
    _CLEANUP_DONE["ran"] = True
    print("[cleanup] tearing down orphaned processes/resources")

    # 1. Remote OpenWhisk port-forward (only ever started for ow_host=127.0.0.1).
    try:
        pid_file = getattr(args, "remote_ow_port_forward_pid_file", None)
        if pid_file and getattr(args, "ow_host", "") == "127.0.0.1":
            ssh_command(
                args,
                f'if [ -f {shlex.quote(pid_file)} ]; then '
                f'kill "$(cat {shlex.quote(pid_file)})" 2>/dev/null || true; '
                f'rm -f {shlex.quote(pid_file)}; fi',
                timeout=30,
            )
    except Exception as exc:  # noqa: BLE001
        print(f"[cleanup] port-forward teardown failed (ignored): {exc}")

    # 2. Spark apps / stale JVMs on the master pod.
    try:
        if "spark" in getattr(args, "backend_list", []):
            _spark_kill_remote_shell(args)
    except Exception as exc:  # noqa: BLE001
        print(f"[cleanup] spark teardown failed (ignored): {exc}")

    # 3. MPI orted/mpirun orphans on every MPI host (cross-host runs leave
    #    orted daemons on the remote node when mpirun is killed).
    try:
        from cost_backends import mpi_host_names
        hosts = mpi_host_names(args)
        if hosts and any(b in getattr(args, "backend_list", []) for b in ("mpi",)):
            pkill = "pkill -9 -f 'orted|mpirun' || true"
            # local pkill on the SSH target, then hop to the other hosts.
            remote = pkill + "; " + "; ".join(
                f"ssh -o BatchMode=yes -o StrictHostKeyChecking=no "
                f"-o ConnectTimeout=10 {shlex.quote(h)} {shlex.quote(pkill)} 2>/dev/null || true"
                for h in hosts if h != getattr(args, "cloudlab_host", "")
            )
            ssh_command(args, remote, timeout=90)
    except Exception as exc:  # noqa: BLE001
        print(f"[cleanup] mpi teardown failed (ignored): {exc}")

    # 4. Leftover guest/prewarm action pods.
    try:
        if "burst" in getattr(args, "backend_list", []):
            from preflight_gate import reap_guest_pods
            reap_guest_pods(args)
    except Exception as exc:  # noqa: BLE001
        print(f"[cleanup] guest-pod reap failed (ignored): {exc}")


def _install_signal_handlers(args: argparse.Namespace) -> None:
    """Run cleanup_all on SIGINT/SIGTERM, then exit. atexit covers normal exit
    and unhandled exceptions so Ctrl-C never leaves orphans behind."""
    def _handler(signum, _frame):
        print(f"\n[signal] received {signal.Signals(signum).name}; cleaning up before exit")
        cleanup_all(args)
        raise SystemExit(128 + signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            pass  # not in main thread, e.g. under a test harness
    atexit.register(lambda: cleanup_all(args))


def main() -> None:
    args = parse_args()
    algo = ALGORITHMS[args.algorithm]

    if not args.dry_run and (not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY")):
        raise SystemExit("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for CloudLab MinIO")

    args.campaign_root.mkdir(parents=True, exist_ok=True)
    args.campaign_root = args.campaign_root.resolve()
    fresh_metadata = {
        "algorithm": algo.name,
        "campaign_root": str(args.campaign_root),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backends": args.backend_list,
        "burst_partition_list": args.burst_partition_list,
        "characterization_partition_list": args.characterization_partition_list,
        "characterization_probes": args.characterization_probes,
        "spark_partition_list": args.spark_partition_list,
        "spark_partition_executor_map": {str(k): v for k, v in args.spark_partition_executor_map.items()},
        "spark_config_memories": args.spark_config_memories,
        "size_nodes": args.size_nodes,
        "chunk_probe_sizes": args.chunk_probe_size_list,
        "cost_sweep_nodes": args.cost_sweep_node_list,
        "cost_runs": args.cost_runs,
        "rayon_threads": args.rayon_thread_list,
        "mpi_ranks": args.mpi_rank_list,
        "mpi_hosts": args.mpi_hosts,
        "mpi_map_by": args.mpi_map_by,
        "mpi_btl_if_include": args.mpi_btl_if_include,
        "burst_effective_invokers": args.burst_effective_invokers,
        "burst_effective_user_memory_mb": args.burst_effective_user_memory_mb,
        "bucket": args.bucket,
        "burst_key_prefix": algo_burst_key_prefix(args, algo),
        "spark_key_prefix": algo_spark_key_prefix(args, algo),
        "host_s3_endpoint": args.host_s3_endpoint,
        "worker_s3_endpoint": args.worker_s3_endpoint,
        "cloudlab_src_root": args.cloudlab_src_root,
        "metadata_authoritative_phases": ["full", "preflight"],
    }
    _persist_metadata(args.campaign_root / "metadata.json", fresh_metadata, args.phase)

    burst_phase_enabled = algo.has_burst_action and "burst" in args.backend_list
    spark_phase_enabled = algo.has_spark_submitter and "spark" in args.backend_list
    cost_phase_enabled = any(b in args.backend_list for b in ("standalone", "rayon", "mpi"))

    cluster_phase = args.phase in {"full", "preflight", "characterization", "config", "chunk_probe", "size", "cost"}

    # Dry-run: plan + fit-check every cell, then exit without touching the cluster.
    if args.dry_run:
        _dry_run_plan(args, algo, burst_phase_enabled, spark_phase_enabled, cost_phase_enabled)
        return

    if args.resume:
        # Resume is idempotent re-execution: cached PASSED cells are skipped,
        # while failed/blocked cells (no valid cache) are re-run. No separate
        # code path is needed — the per-cell cache provides resume semantics.
        print(f"[resume] re-running phase '{args.phase}' for {algo.display_name}; "
              f"cached passes will be skipped, failures re-run.")

    # From here on the run touches the cluster: arm cleanup so SIGINT/SIGTERM
    # or any exit tears down orphans (port-forward, Spark, MPI orted, guest pods).
    if cluster_phase:
        _install_signal_handlers(args)

    # Cluster health gate: detect + auto-remediate inconsistent OpenWhisk/Burst
    # state and foreign-tenant load BEFORE doing any work. Runs once per algo
    # invocation, which is also the boundary where past campaigns drifted
    # (zombies/broken cells left between algorithms).
    if cluster_phase and (burst_phase_enabled or cost_phase_enabled) and not args.skip_preflight:
        try:
            ensure_cluster_ready(args, phase=args.phase, remediate=not args.preflight_detect_only)
        except PreflightError as exc:
            raise SystemExit(f"[preflight] ABORT: {exc}")

    if cluster_phase:
        sync_remote_scripts(args, algo)
        if burst_phase_enabled:
            ensure_remote_openwhisk_api(args, args.campaign_root)

    # Preflight (Burst smoke). Skip when burst phase disabled.
    if args.phase in {"full", "preflight"} and burst_phase_enabled:
        _set_burst_partition(args, args.burst_partition_list[0])
        graph_file = ensure_local_graph_file(
            args, algo, args.campaign_root,
            nodes=args.preflight_nodes, partitions=args.burst_partitions,
        )
        run_preflight(args, algo, args.campaign_root, graph_file)

    # Characterization, config_sweep, chunk_probe: Burst-only phases.
    all_winners: dict[int, dict[str, Any]] = {}
    if burst_phase_enabled:
        for bp in args.characterization_partition_list:
            _set_burst_partition(args, bp)
            if args.phase in {"full", "characterization"}:
                graph_file = ensure_local_graph_file(
                    args, algo, args.campaign_root,
                    nodes=args.characterization_nodes, partitions=bp,
                )
                run_characterization(args, algo, args.campaign_root, graph_file)

        for bp in args.burst_partition_list:
            _set_burst_partition(args, bp)
            if args.phase in {"full", "config"}:
                graph_file = ensure_local_graph_file(
                    args, algo, args.campaign_root,
                    nodes=args.config_nodes, partitions=bp,
                )
                winners = run_config_sweep(args, algo, args.campaign_root, graph_file)
                write_json(_best_config_path(args.campaign_root, algo.name, bp), winners)
                all_winners[bp] = winners

        if args.phase in {"full", "chunk_probe"}:
            if not all_winners:
                all_winners = _load_all_winners(args, algo.name)
            first_bp = args.burst_partition_list[0]
            if first_bp in all_winners:
                _set_burst_partition(args, first_bp)
                run_chunk_probe(args, algo, args.campaign_root, all_winners[first_bp])

        if args.phase in {"full", "config"} and all_winners:
            write_json(
                args.campaign_root / "config_sweep" / f"best_config_{algo.name}.json",
                {str(bp): w for bp, w in all_winners.items()},
            )

    # Size sweep: Burst and/or Spark depending on --backends. Allowed even
    # when burst is disabled (e.g. running Spark-only against a campaign
    # that already has burst data cached from a previous pass).
    if args.phase in {"full", "size"} and (burst_phase_enabled or spark_phase_enabled):
        if not all_winners:
            all_winners = _load_all_winners(args, algo.name)
        for bp in args.burst_partition_list:
            _set_burst_partition(args, bp)
            if bp not in all_winners:
                cfg = _read_best_config(args.campaign_root, algo.name, bp)
                if cfg is not None:
                    all_winners[bp] = cfg
                elif burst_phase_enabled:
                    raise RuntimeError(
                        f"missing config_sweep/best_config_{algo.name}_p{bp}.json "
                        f"(also no legacy best_config_p{bp}.json); run config phase first"
                    )
                else:
                    all_winners[bp] = {}
            # Inject a default Spark winner if Spark is enabled and the loaded
            # best_config_pN.json doesn't include one (typical for campaigns
            # whose config phase only ran for Burst).
            if spark_phase_enabled and not all_winners[bp].get("spark"):
                all_winners[bp]["spark"] = {
                    "executors": args.spark_partition_executor_map.get(bp, args.spark_executor_list[0]),
                    "executor_memory": args.spark_config_memories[0],
                }
            if bp in args.spark_partition_executor_map:
                _set_spark_partition(args, bp)
            else:
                _set_spark_partition(args, args.spark_partition_list[0])
            run_size_sweep(args, algo, args.campaign_root, all_winners[bp])

    elif args.phase in {"preflight", "characterization", "config", "chunk_probe", "size"} and not burst_phase_enabled and not spark_phase_enabled:
        print(f"[skip] {algo.display_name}: neither burst nor spark in --backends; skipping {args.phase} phase.")

    # COST sweep (standalone/rayon/mpi) — independent of burst/spark winners
    if args.phase in {"full", "cost"} and cost_phase_enabled:
        run_cost_sweep(args, algo, args.campaign_root)

    # Auto report (MD tables + PNG figures)
    if args.phase in {"full", "report"}:
        run_report_phase(args, algo, args.campaign_root)

    # Post-run health scan: surface every cell that did not pass + a resume cmd.
    incomplete = report_campaign_health(args, algo, args.campaign_root)

    print(f"\n{'='*60}")
    print(f"Campaign {algo.display_name} complete: {args.campaign_root}")
    if incomplete:
        print(f"  WARNING: {incomplete} incomplete cell(s) — see [health] above to resume.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
