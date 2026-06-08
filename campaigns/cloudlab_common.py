#!/usr/bin/env python3
"""Shared infrastructure for CloudLab campaign runners.

Reconstructed from data shapes, campaign report, and call-sites in
run_cloudlab_campaign.py. Original source was lost; this module rebuilds
the same surface area so that run_cloudlab_campaign.py can import it
unchanged.

Functional behavior matches the original within the limits of inference
from artifacts (raw_runs JSONs, summary JSONs, runtime_probe outputs,
plan_campana_cloudlab_multi_algoritmo.md). Not byte-identical to the
original. Validated against existing campaign outputs (data shapes
match), not against a fresh CloudLab execution.
"""
from __future__ import annotations

import json
import math
import os
import re
import shlex
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT_ROOT = ROOT / "experiment_data" / "cloudlab_campaigns"
BENCHMARK_PREFIX = "BENCHMARK_RESULT_JSON:"
REMOTE_DEFAULT_SRC_ROOT = "/home/users/sconde/src"

# infra/resource_capacity.py lives one level up; expose its types here so the
# orchestrator gets a single import surface for resource fit-checking.
_INFRA_DIR = ROOT / "infra"
if str(_INFRA_DIR) not in sys.path:
    sys.path.insert(0, str(_INFRA_DIR))
from resource_capacity import (  # noqa: E402
    HostCapacity,
    HostBudget,
    ResourceRequest,
    burst_cluster_request,
    spark_cluster_request,
    parse_memory_to_mb,
)

# CloudLab worker nodes (compute6/compute7) physical caps. mpi-hosts declares
# 32 slots/host → 32 logical CPUs; nodes carry 64 GiB RAM.
CLOUDLAB_NODE_TOTAL_MEMORY_MB = 64 * 1024
CLOUDLAB_NODE_LOGICAL_CPUS = 32
# Headroom left for the OS + OpenWhisk control plane (controller/invoker/couchdb
# co-resident on the worker) so a benchmark cell can't starve the cluster.
CLOUDLAB_NODE_RESERVED_MEMORY_MB = 8 * 1024
CLOUDLAB_NODE_RESERVED_CPUS = 4


# ---------------------------------------------------------------------------
# Utility primitives
# ---------------------------------------------------------------------------

def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def read_json(path: Path) -> Any:
    return json.loads(Path(path).read_text())


def mean_std(values: Iterable[float]) -> tuple[float, float]:
    vals = [float(v) for v in values]
    if not vals:
        return 0.0, 0.0
    mean = sum(vals) / len(vals)
    if len(vals) == 1:
        return mean, 0.0
    return mean, statistics.stdev(vals)


def shell_quote_env(value: str) -> str:
    return shlex.quote(value)


def _tee_log(text: str, log_path: Path | None) -> None:
    if log_path is None:
        return
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(text)


def run_command(
    cmd: list[str],
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    log_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        cmd, cwd=cwd, env=env, timeout=timeout,
        capture_output=True, text=True, check=False,
    )
    if log_path is not None:
        header = f"$ {' '.join(shlex.quote(c) for c in cmd)}\n"
        _tee_log(header + (completed.stdout or "") + (completed.stderr or ""), log_path)
    return completed


# ---------------------------------------------------------------------------
# SSH / SCP wrappers
# ---------------------------------------------------------------------------

def _ssh_base(args) -> list[str]:
    return [
        "ssh",
        *_ssh_config_args(args),
        "-i", args.cloudlab_ssh_key,
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "ServerAliveInterval=30",
        f"{args.cloudlab_user}@{args.cloudlab_host}",
    ]


def _ssh_config_args(args) -> list[str]:
    cfg = getattr(args, "cloudlab_ssh_config", None) or os.environ.get("CLOUDLAB_SSH_CONFIG")
    return ["-F", cfg] if cfg else []


def ssh_command(
    args, command: str,
    timeout: int | None = None,
    log_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = _ssh_base(args) + [command]
    return run_command(cmd, timeout=timeout, log_path=log_path)


def ssh_python(
    args, script: str,
    timeout: int | None = None,
    log_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = _ssh_base(args) + ["python3 -"]
    completed = subprocess.run(
        cmd, input=script, capture_output=True, text=True,
        timeout=timeout, check=False,
    )
    if log_path is not None:
        _tee_log(
            f"$ ssh python3 - <<EOF\n{script}\nEOF\n"
            + (completed.stdout or "") + (completed.stderr or ""),
            log_path,
        )
    return completed


def scp_to_remote(args, local: Path, remote: str) -> subprocess.CompletedProcess[str]:
    cmd = [
        "scp",
        *_ssh_config_args(args),
        "-i", args.cloudlab_ssh_key,
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        str(local),
        f"{args.cloudlab_user}@{args.cloudlab_host}:{remote}",
    ]
    return run_command(cmd, timeout=600)


def scp_from_remote(args, remote: str, local: Path) -> subprocess.CompletedProcess[str]:
    Path(local).parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "scp",
        *_ssh_config_args(args),
        "-i", args.cloudlab_ssh_key,
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        f"{args.cloudlab_user}@{args.cloudlab_host}:{remote}",
        str(local),
    ]
    return run_command(cmd, timeout=600)


# ---------------------------------------------------------------------------
# Stdout parsing
# ---------------------------------------------------------------------------

def parse_prefixed_json(stdout: str) -> dict[str, Any]:
    for line in (stdout or "").splitlines():
        if line.startswith(BENCHMARK_PREFIX):
            payload = line[len(BENCHMARK_PREFIX):].strip()
            return json.loads(payload)
    raise ValueError(f"no line starting with {BENCHMARK_PREFIX!r} in stdout")


def parse_json_lines(stdout: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in (stdout or "").splitlines():
        line = raw.strip()
        if not line.startswith("{"):
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def has_openwhisk_transport_failure(stdout: str | None, stderr: str | None) -> bool:
    blob = (stdout or "") + "\n" + (stderr or "")
    markers = (
        "ConnectionResetError",
        "Connection reset by peer",
        "Remote end closed connection",
        "RemoteDisconnected",
        "Connection refused",
        "BadStatusLine",
        "ProtocolError",
        "openwhisk transport",
        "EOF occurred in violation of protocol",
    )
    return any(m in blob for m in markers)


# ---------------------------------------------------------------------------
# Burst config matrix and partition / granularity policy
# ---------------------------------------------------------------------------

def valid_granularities(partitions: int) -> list[int]:
    """Granularities g such that p % g == 0 and p/g >= 2 (≥2 workers per pack)."""
    return [g for g in range(2, partitions + 1) if partitions % g == 0 and partitions // g >= 2]


def characterization_granularities(partitions: int, algorithm: str | None = None) -> list[int]:
    """Granularities used during the microcharacterization phase."""
    return valid_granularities(partitions)


def parse_burst_config_matrix(partitions: int) -> dict[int, list[int]]:
    """Burst (granularity → memories) matrix per partition count.

    Source: doc-tfm/plan_campana_cloudlab_multi_algoritmo.md, sections 1a/1c.
    """
    if partitions == 4:
        return {2: [2048, 3072, 4096]}
    if partitions == 8:
        return {2: [3072], 4: [2048, 3072, 4096]}
    if partitions == 16:
        return {4: [2048, 3072], 8: [2048, 3072, 4096]}
    return {g: [2048, 3072, 4096] for g in valid_granularities(partitions)}


# ---------------------------------------------------------------------------
# Resource sizing and pre-launch fit-check
# ---------------------------------------------------------------------------

# Per-worker Burst memory tiers, keyed by graph size n. Calibrated to the
# measured heap + S3-chunk message-buffer footprint of the Burst runtime:
# campaign-0604 OOM'd at n=10M with m=2048 and only passed once bumped to 4096
# (see project_campaign_0604 memory). n>5M gets 6144 for tail safety margin.
# This REPLACES the hard-coded m=4096 stub previously injected by
# launch_campaign_v3.sh, scaling memory with n instead of a single flat value.
_BURST_MEM_TIERS_MB: tuple[tuple[float, int], ...] = (
    (100_000, 2048),
    (1_000_000, 3072),
    (5_000_000, 4096),
    (float("inf"), 6144),
)
_BURST_MEM_FLOOR_MB = 2048


def burst_memory_mb(
    algorithm: str,
    nodes: int,
    *,
    budget_mb: int | None = None,
    floor_mb: int = _BURST_MEM_FLOOR_MB,
) -> int:
    """Per-worker Burst action memory (MiB) scaled by graph size n.

    ``algorithm`` is accepted for forward-compatibility (per-algo factors) but
    currently every algorithm uses the same conservative tier — the admission
    budget is large and over-provisioning memory does not perturb timings,
    whereas under-provisioning OOMs the cell. ``budget_mb`` clamps to the
    single-invoker user-memory budget so the value can never exceed admission.
    """
    base = next(mem for threshold, mem in _BURST_MEM_TIERS_MB if nodes <= threshold)
    mem = max(floor_mb, base)
    if budget_mb is not None:
        mem = min(mem, budget_mb)
    return mem


def cloudlab_node_budget(
    *,
    total_memory_mb: int = CLOUDLAB_NODE_TOTAL_MEMORY_MB,
    logical_cpus: int = CLOUDLAB_NODE_LOGICAL_CPUS,
    reserved_memory_mb: int = CLOUDLAB_NODE_RESERVED_MEMORY_MB,
    reserved_cpus: int = CLOUDLAB_NODE_RESERVED_CPUS,
) -> HostBudget:
    """HostBudget for a single CloudLab worker node (compute6/compute7)."""
    return HostBudget(
        host=HostCapacity(logical_cpus=logical_cpus, total_memory_mb=total_memory_mb),
        reserved_cpus=reserved_cpus,
        reserved_memory_mb=reserved_memory_mb,
    )


def burst_cell_fit(
    *,
    partitions: int,
    granularity: int,
    memory_mb: int,
    budget: HostBudget | None = None,
) -> tuple[bool, ResourceRequest]:
    """Check whether a Burst cell fits a single worker node.

    ``--burst-effective-invokers`` defaults to 1, so all ``partitions`` workers
    co-reside on one invoker/node. Returns (fits, request) so callers can both
    gate and log the request shape.
    """
    budget = budget or cloudlab_node_budget()
    request = burst_cluster_request(
        workers=partitions,
        memory_per_worker_mb=memory_mb,
        system_reserved_cpus=CLOUDLAB_NODE_RESERVED_CPUS,
        system_reserved_mem_mb=CLOUDLAB_NODE_RESERVED_MEMORY_MB,
    )
    return request.fits(budget), request


def spark_cell_fit(
    *,
    executors: int,
    executor_memory: str | int,
    budget: HostBudget | None = None,
) -> tuple[bool, ResourceRequest]:
    """Check whether a Spark cell fits a single worker node."""
    budget = budget or cloudlab_node_budget()
    request = spark_cluster_request(
        executors=executors,
        executor_memory=executor_memory,
    )
    return request.fits(budget), request


def split_workers(total_executors: int) -> tuple[int, int]:
    """Split total executors evenly between compute6 and compute7."""
    half = total_executors // 2
    return half, total_executors - half


_SPARK_MEMORY_RE = re.compile(r"^(\d+)([gGmM])$")


def spark_memory_to_k8s(value: str) -> str:
    """'4g' → '4Gi', '512m' → '512Mi'."""
    m = _SPARK_MEMORY_RE.match(value.strip())
    if not m:
        return value
    number, unit = m.group(1), m.group(2).lower()
    suffix = {"g": "Gi", "m": "Mi"}[unit]
    return f"{number}{suffix}"


# ---------------------------------------------------------------------------
# Summarization
# ---------------------------------------------------------------------------

def _burst_metrics(row: dict[str, Any]) -> dict[str, float]:
    r = row.get("result", {})
    burst = r.get("burst", r) if isinstance(r, dict) else {}
    e2e = float(burst.get("end_to_end_ms") or burst.get("total_time_ms") or 0)
    warm_subtotal = float(burst.get("warm_total_ms") or 0)
    return {"warm": e2e, "e2e": e2e, "cold": e2e, "warm_subtotal": warm_subtotal}


def _spark_metrics(row: dict[str, Any]) -> dict[str, float]:
    r = row.get("result", {})
    e2e = float(r.get("end_to_end_ms") or r.get("total_time_ms") or 0)
    return {"warm": e2e, "e2e": e2e, "cold": e2e, "warm_subtotal": e2e}


def _group_key(row: dict[str, Any]) -> tuple:
    framework = row.get("framework")
    if framework == "burst":
        return (
            "burst",
            row.get("algorithm"),
            row.get("nodes"),
            row.get("partitions"),
            row.get("granularity"),
            row.get("memory_mb"),
        )
    return (
        "spark",
        row.get("algorithm"),
        row.get("nodes"),
        row.get("partitions"),
        row.get("executors"),
        row.get("executor_memory"),
    )


def summarize_rows(
    rows: list[dict[str, Any]],
    metric_extractor: Callable[[dict[str, Any]], float] | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple, list[dict[str, Any]]] = {}
    for r in rows:
        if r.get("status") != "passed":
            continue
        groups.setdefault(_group_key(r), []).append(r)

    summary: list[dict[str, Any]] = []
    for key, group in groups.items():
        framework = key[0]
        default_extractor = _burst_metrics if framework == "burst" else _spark_metrics
        if metric_extractor is not None:
            primaries = [float(metric_extractor(r)) for r in group]
        else:
            primaries = [default_extractor(r)["e2e"] for r in group]
        warms = [default_extractor(r)["warm"] for r in group]
        e2es = [default_extractor(r)["e2e"] for r in group]
        colds = [default_extractor(r)["cold"] for r in group]
        warm_subtotals = [default_extractor(r).get("warm_subtotal", 0.0) for r in group]
        primary_mean = sum(primaries) / len(primaries) if primaries else 0.0
        warm_mean = sum(warms) / len(warms) if warms else 0.0
        e2e_mean = sum(e2es) / len(e2es) if e2es else 0.0
        cold_mean = sum(colds) / len(colds) if colds else 0.0
        warm_subtotal_mean = sum(warm_subtotals) / len(warm_subtotals) if warm_subtotals else 0.0
        e2e_std = statistics.pstdev(e2es) if len(e2es) > 1 else 0.0

        sample = group[0]
        record: dict[str, Any] = {
            "algorithm": sample.get("algorithm"),
            "framework": framework,
            "nodes": sample.get("nodes"),
            "partitions": sample.get("partitions"),
            "runs": len(group),
            "mean_primary_metric_ms": primary_mean,
            "mean_warm_total_ms": warm_mean,
            "mean_end_to_end_ms": e2e_mean,
            "mean_cold_total_ms": cold_mean,
            "mean_warm_subtotal_ms": warm_subtotal_mean,
            "std_end_to_end_ms": e2e_std,
        }
        if framework == "burst":
            record["granularity"] = sample.get("granularity")
            record["memory_mb"] = sample.get("memory_mb")
        else:
            record["executors"] = sample.get("executors")
            record["executor_memory"] = sample.get("executor_memory")
        summary.append(record)

    summary.sort(key=lambda x: (x.get("nodes") or 0, x.get("granularity") or 0,
                                x.get("memory_mb") or 0))
    return summary


def pick_best_config(summary: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not summary:
        return None
    return min(
        summary,
        key=lambda r: r.get("mean_primary_metric_ms")
        or r.get("mean_end_to_end_ms", float("inf")),
    )


def failed_run_record(**kwargs) -> dict[str, Any]:
    record = {"status": "failed"}
    record.update(kwargs)
    record.setdefault("framework", kwargs.get("framework", "burst"))
    record.setdefault("algorithm", kwargs.get("algorithm"))
    return record


def read_cached_record(path: Path) -> dict[str, Any] | None:
    """Read a cached raw-run JSON, returning None on a missing/corrupt/truncated
    file instead of raising. A partial write from an interrupted run must read as
    a cache miss (regenerate), not crash the phase."""
    try:
        rec = read_json(path)
    except Exception:
        return None
    return rec if isinstance(rec, dict) else None


def cached_record_ok(cached: Any) -> bool:
    """Whether a cached record can be trusted as a completed cell. Rejects
    failed/blocked records (so a resume re-runs them) and 'passed' records that
    carry no result payload (corruption). Measured non-pass statuses (e.g.
    'timeout_900s') are valid structural findings and kept."""
    if not isinstance(cached, dict):
        return False
    status = cached.get("status")
    if status in ("failed", "blocked"):
        return False
    if status in ("passed", "ok"):
        return bool(cached.get("result"))
    # Legacy records without an explicit status, or measured timeout statuses.
    return bool(cached.get("result")) or status is not None


def summarize_runtime_probes(probe_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_mode: dict[str, list[dict[str, Any]]] = {}
    for row in probe_rows:
        mode = row.get("probe") or row.get("mode")
        if not mode:
            continue
        by_mode.setdefault(mode, []).append(row)

    summary: dict[str, Any] = {}
    for mode, rows in by_mode.items():
        per_g: dict[int, list[dict[str, Any]]] = {}
        for r in rows:
            g = r.get("granularity") or r.get("result", {}).get("configuration", {}).get("granularity")
            if g is None:
                continue
            per_g.setdefault(int(g), []).append(r)
        per_granularity = {}
        for g, rs in per_g.items():
            host_totals = []
            startup_medians = []
            staggers = []
            for r in rs:
                metrics = (r.get("result", {}) or {}).get("metrics", {})
                if "host_total_ms" in metrics:
                    host_totals.append(float(metrics["host_total_ms"]))
                if "startup_median_ms" in metrics:
                    startup_medians.append(float(metrics["startup_median_ms"]))
                if "stagger_ms" in metrics:
                    staggers.append(float(metrics["stagger_ms"]))
            entry: dict[str, Any] = {"runs": len(rs)}
            if host_totals:
                entry["mean_host_total_ms"] = sum(host_totals) / len(host_totals)
            if startup_medians:
                entry["mean_startup_median_ms"] = sum(startup_medians) / len(startup_medians)
            if staggers:
                entry["mean_stagger_ms"] = sum(staggers) / len(staggers)
            per_granularity[str(g)] = entry
        summary[mode] = per_granularity
    return summary


# ---------------------------------------------------------------------------
# Remote OW + S3 management
# ---------------------------------------------------------------------------

def ensure_remote_openwhisk_api(args, campaign_root: Path) -> None:
    """Ensure a port-forward exists from local args.ow_port to the OW gateway.

    Skipped when ``args.ow_host`` is not ``127.0.0.1`` — that signals the
    caller wants to reach OW directly (e.g. via the cluster's nginx
    ClusterIP from a Kubernetes worker node), so no port-forward is needed.
    """
    if args.ow_host != "127.0.0.1":
        return
    pid_file = args.remote_ow_port_forward_pid_file
    log_file = args.remote_ow_port_forward_log_file
    namespace = shlex.quote(args.ow_namespace)
    release = shlex.quote(args.ow_release_name)
    port = int(args.ow_port)
    script = f"""
set -euo pipefail
PID_FILE={shlex.quote(pid_file)}
LOG_FILE={shlex.quote(log_file)}
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    exit 0
fi
SVC=$(kubectl -n {namespace} get svc -l app.kubernetes.io/name=openwhisk -o jsonpath='{{.items[?(@.spec.type=="NodePort")].metadata.name}}' 2>/dev/null | awk '{{print $1}}')
SVC=${{SVC:-{release}-nginx}}
nohup kubectl -n {namespace} port-forward --address 127.0.0.1 svc/"$SVC" {port}:80 \\
    > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
sleep 3
"""
    completed = ssh_command(args, script, timeout=60)
    if completed.returncode != 0:
        log_path = campaign_root / "logs" / "ow_port_forward.log"
        _tee_log(completed.stdout + completed.stderr, log_path)


def remote_lp_partitions_available(args, bucket: str, prefix: str, partitions: int) -> bool:
    script = f"""
import json
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url={json.dumps(args.host_s3_endpoint)},
    aws_access_key_id={json.dumps(os.environ.get('AWS_ACCESS_KEY_ID', ''))},
    aws_secret_access_key={json.dumps(os.environ.get('AWS_SECRET_ACCESS_KEY', ''))},
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)
expected = {{f"{prefix}/part-{{i:05d}}" for i in range({partitions})}}
present = set()
for page in s3.get_paginator("list_objects_v2").paginate(Bucket={json.dumps(bucket)}, Prefix={json.dumps(prefix + '/part-')}):
    for item in page.get("Contents", []):
        key = item.get("Key")
        if key and item.get("Size", 0) > 0:
            present.add(key)
print(json.dumps({{"ready": present == expected, "have": len(present), "want": len(expected)}}))
"""
    completed = ssh_python(args, script, timeout=120)
    if completed.returncode != 0:
        return False
    try:
        for line in completed.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                payload = json.loads(line)
                return bool(payload.get("ready"))
    except Exception:
        return False
    return False


def remote_delete_prefix(args, bucket: str, prefix: str) -> None:
    script = f"""
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url={json.dumps(args.host_s3_endpoint)},
    aws_access_key_id={json.dumps(os.environ.get('AWS_ACCESS_KEY_ID', ''))},
    aws_secret_access_key={json.dumps(os.environ.get('AWS_SECRET_ACCESS_KEY', ''))},
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)
deleted = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket={json.dumps(bucket)}, Prefix={json.dumps(prefix)}):
    for item in page.get("Contents", []):
        s3.delete_object(Bucket={json.dumps(bucket)}, Key=item["Key"])
        deleted += 1
print(deleted)
"""
    ssh_python(args, script, timeout=300)


# ---------------------------------------------------------------------------
# Resource snapshots (kubectl top)
# ---------------------------------------------------------------------------

def capture_resource_snapshot(args, campaign_root: Path, phase: str, label: str) -> None:
    out_dir = campaign_root / "resource_snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = utc_stamp()
    cmd = (
        f"kubectl top nodes --no-headers 2>/dev/null; echo '---PODS---'; "
        f"kubectl -n {shlex.quote(args.ow_namespace)} top pods --no-headers 2>/dev/null"
    )
    completed = ssh_command(args, cmd, timeout=60)
    payload = {
        "phase": phase,
        "label": label,
        "timestamp_utc": timestamp,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "returncode": completed.returncode,
    }
    write_json(out_dir / f"{phase}_{label}_{timestamp}.json", payload)


# ---------------------------------------------------------------------------
# Runtime probe
# ---------------------------------------------------------------------------

def run_runtime_probe(
    args, campaign_root: Path, *,
    mode: str, workers: int, granularity: int,
    run_index: int, load_key_prefix: str | None = None,
) -> dict[str, Any]:
    raw_path = (
        campaign_root / "raw_runs" / "runtime_probe"
        / f"probe_{mode}_w{workers}_g{granularity}_run{run_index}.json"
    )
    if raw_path.exists():
        return read_json(raw_path)

    log_path = (
        campaign_root / "logs" / "characterization"
        / f"probe_{mode}_w{workers}_g{granularity}_run{run_index}.log"
    )

    bench_args = [
        "python3", "benchmark_runtime_probe.py",
        "--mode", mode,
        "--workers", str(workers),
        "--granularity", str(granularity),
        "--memory", str(args.characterization_memory_mb),
        "--iterations", str(args.characterization_iterations),
        "--payload-bytes", str(args.characterization_payload_bytes),
        "--ow-host", args.ow_host,
        "--ow-port", str(args.ow_port),
        "--ow-protocol", shlex.quote(args.ow_protocol),
        "--backend", shlex.quote(args.backend),
        "--chunk-size", str(args.chunk_size_kb),
        "--bucket", shlex.quote(args.bucket),
        "--s3-endpoint", shlex.quote(args.worker_s3_endpoint),
    ]
    if load_key_prefix:
        bench_args += ["--key-prefix", shlex.quote(load_key_prefix)]

    remote_dir = f"{args.cloudlab_src_root}/labelpropagation"
    remote_command = " ".join([
        "cd", shlex.quote(remote_dir), "&&",
        "env",
        f"AWS_ACCESS_KEY_ID={shell_quote_env(os.environ['AWS_ACCESS_KEY_ID'])}",
        f"AWS_SECRET_ACCESS_KEY={shell_quote_env(os.environ['AWS_SECRET_ACCESS_KEY'])}",
        f"OW_PROTOCOL={shlex.quote(args.ow_protocol)}",
        f"OPENWHISK_K8S_NAMESPACE={shlex.quote(args.ow_namespace)}",
        f"OPENWHISK_RELEASE_NAME={shlex.quote(args.ow_release_name)}",
        "timeout", "1200s",
    ] + bench_args)

    completed = ssh_command(args, remote_command, timeout=1500, log_path=log_path)
    result: dict[str, Any] | None = None
    if completed.returncode == 0:
        try:
            result = parse_prefixed_json(completed.stdout)
        except ValueError:
            lines = parse_json_lines(completed.stdout)
            for line in reversed(lines):
                if "metrics" in line or "probe" in line:
                    result = line
                    break
    if result is None:
        raise RuntimeError(f"runtime probe ({mode}, g={granularity}) did not return result, see {log_path}")

    record = {
        "phase": "characterization",
        "framework": "burst",
        "probe": mode,
        "granularity": granularity,
        "result": result,
        "log_path": str(log_path),
    }
    write_json(raw_path, record)
    return record


# ---------------------------------------------------------------------------
# Helper used by run_cloudlab_campaign.run_config_sweep
# ---------------------------------------------------------------------------

def _set_spark_partition(args, partitions: int) -> None:
    args.spark_partitions = partitions
    args.spark_total_executors = args.spark_partition_executor_map.get(
        partitions, args.spark_executor_list[0]
    )


__all__ = [
    "ROOT", "EXPERIMENT_ROOT", "BENCHMARK_PREFIX", "REMOTE_DEFAULT_SRC_ROOT",
    "utc_stamp", "valid_granularities", "characterization_granularities",
    "write_json", "read_json", "mean_std",
    "run_command", "ssh_command", "scp_to_remote", "scp_from_remote", "ssh_python",
    "shell_quote_env",
    "parse_prefixed_json", "parse_json_lines", "has_openwhisk_transport_failure",
    "split_workers", "spark_memory_to_k8s", "parse_burst_config_matrix",
    "summarize_rows", "failed_run_record", "summarize_runtime_probes", "pick_best_config",
    "remote_delete_prefix", "remote_lp_partitions_available",
    "ensure_remote_openwhisk_api", "capture_resource_snapshot",
    "run_runtime_probe",
    "_set_spark_partition",
]
