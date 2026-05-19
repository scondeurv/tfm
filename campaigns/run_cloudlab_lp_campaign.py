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
        "-i", args.cloudlab_ssh_key,
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "ServerAliveInterval=30",
        f"{args.cloudlab_user}@{args.cloudlab_host}",
    ]


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
    """Ensure a port-forward exists from local args.ow_port to the OW gateway."""
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
        "--ow-host", "127.0.0.1",
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
