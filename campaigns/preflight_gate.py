#!/usr/bin/env python3
"""Pre-launch cluster health gate with auto-remediation.

The campaigns kept failing mid-run because the OpenWhisk/Burst cluster was left
in an inconsistent state by a previous run (zombie guest pods, stuck activations,
a controller/invoker wedged on the ``wait-for-couchdb`` 404), or because a
foreign tenant was saturating the worker nodes. This module runs BEFORE the
campaign starts and between phases, detects those conditions, and — per the
operator's decision — auto-remediates what it safely can, aborting with a clear
diagnostic only when it cannot.

All cluster interaction goes through ``_kube_ssh`` on the kubectl host (the proxy
that holds ~/.kube/config; the compute workers have none), which has
``kubectl``/``jq``. Detection is best-effort: a probe that itself errors degrades
to a warning, never a crash. Only genuine, confirmed bad states gate.
"""
from __future__ import annotations

import json
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

from cloudlab_common import utc_stamp, _ssh_config_args


def _kube_ssh(args, command: str, *, timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a command on the kubectl host (the proxy that holds ~/.kube/config),
    NOT the compute worker — the workers have no kubeconfig. Mirrors the Spark
    path's --spark-kubectl-host so all cluster control goes through one place."""
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
    cmd += [f"{user}@{kube_host}", command]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)


class PreflightError(RuntimeError):
    """Raised when the cluster is not ready and cannot be auto-remediated."""


# Namespaces that are "ours" or cluster infrastructure (not a tenant workload).
# Everything else on this shared research cluster is another tenant. We only
# *report* foreign load — on a permanently multi-tenant cluster, refusing to run
# because neighbours exist would be wrong; the operator decides from the detail.
_OWN_NAMESPACES = {
    "openwhisk", "kube-system", "kube-public", "kube-node-lease", "default",
    "cert-manager", "local-path-storage", "prometheus-system", "knative-serving",
    "kubernetes-dashboard", "metallb-system",
}
_OWN_NAMESPACE_PREFIXES = ("spark", "calico", "tigera")

# A node consuming more than this fraction of CPU or memory before we even start
# is a red flag (foreign tenant or leaked pods from a crashed run).
_NODE_BUSY_FRACTION = 0.85


@dataclass
class HealthReport:
    ok: bool = True
    problems: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def fail(self, msg: str) -> None:
        self.ok = False
        self.problems.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def render(self) -> str:
        lines = [f"cluster health: {'OK' if self.ok else 'NOT READY'}"]
        for p in self.problems:
            lines.append(f"  [problem] {p}")
        for w in self.warnings:
            lines.append(f"  [warn]    {w}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Low-level kubectl helpers (run on the CloudLab host via ssh)
# ---------------------------------------------------------------------------

def _kubectl_json(args, kubectl_args: str, *, timeout: int = 60) -> dict[str, Any] | None:
    """Run ``kubectl <kubectl_args> -o json`` remotely; return parsed JSON or None."""
    completed = _kube_ssh(args, f"kubectl {kubectl_args} -o json", timeout=timeout)
    if completed.returncode != 0 or not completed.stdout.strip():
        return None
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        return None


def _pod_ready(pod: dict[str, Any]) -> bool:
    status = pod.get("status", {})
    if status.get("phase") not in ("Running", "Succeeded"):
        return False
    conds = {c.get("type"): c.get("status") for c in status.get("conditions", [])}
    # Jobs reach Succeeded without a Ready condition; treat that as ready.
    if status.get("phase") == "Succeeded":
        return True
    return conds.get("Ready") == "True"


def _name(pod: dict[str, Any]) -> str:
    return pod.get("metadata", {}).get("name", "")


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def check_core_pods(args, report: HealthReport) -> dict[str, Any]:
    """Verify controller / couchdb / kafka / nginx are Ready and count healthy
    invokers. Records details under report.details['core_pods']."""
    ns = args.ow_namespace
    data = _kubectl_json(args, f"-n {shlex.quote(ns)} get pods")
    info: dict[str, Any] = {"healthy_invokers": 0, "components": {}}
    report.details["core_pods"] = info
    if data is None:
        report.fail(f"could not list pods in namespace '{ns}' (kubectl unreachable?)")
        return info

    pods = data.get("items", [])
    release = args.ow_release_name
    # invoker pods: the statefulset owdev-invoker -> owdev-invoker-0, ...
    invoker_prefix = f"{release}-invoker"
    required = {
        "controller": f"{release}-controller",
        "couchdb": f"{release}-couchdb",
        "kafka": f"{release}-kafka",
        "nginx": f"{release}-nginx",
    }
    found_components: dict[str, bool] = {}
    for pod in pods:
        name = _name(pod)
        # invoker management pods (the statefulset), not guest/prewarm workers
        if name.startswith(invoker_prefix) and "guest-" not in name and "prewarm-" not in name:
            if _pod_ready(pod):
                info["healthy_invokers"] += 1
            continue
        for comp, prefix in required.items():
            if name.startswith(prefix) and "invoker" not in name:
                found_components[comp] = found_components.get(comp, False) or _pod_ready(pod)

    info["components"] = found_components
    for comp in required:
        ready = found_components.get(comp)
        if ready is None:
            report.fail(f"core component '{comp}' pod not found")
        elif not ready:
            report.fail(f"core component '{comp}' pod not Ready")
    if info["healthy_invokers"] == 0:
        report.fail("no healthy invoker pods")
    return info


def check_marker_db(args, report: HealthReport) -> str:
    """Probe the couchdb marker DB that ``wait-for-couchdb`` checks. Returns one
    of 'present' | 'missing' | 'unknown'. A 'missing' marker is the root cause of
    the controller/invoker 404 wedge after a restart."""
    ns = args.ow_namespace
    data = _kubectl_json(args, f"-n {shlex.quote(ns)} get pods")
    couch = None
    for pod in (data or {}).get("items", []):
        # A couchdb Deployment can leave an old Completed pod around; only a
        # Running pod is exec-able for the marker probe.
        if (_name(pod).startswith(f"{args.ow_release_name}-couchdb")
                and pod.get("status", {}).get("phase") == "Running"):
            couch = _name(pod)
            break
    if not couch:
        report.warn("couchdb pod not found; cannot verify init marker")
        report.details["marker_db"] = "unknown"
        return "unknown"
    # curl localhost inside the couchdb pod using its own credentials env.
    probe = (
        "curl -s -o /dev/null -w '%{http_code}' "
        '"http://$COUCHDB_USER:$COUCHDB_PASSWORD@127.0.0.1:5984/'
        'ow_kube_couchdb_initialized_marker"'
    )
    completed = _kube_ssh(
        args,
        f"kubectl -n {shlex.quote(ns)} exec {shlex.quote(couch)} -- sh -c {shlex.quote(probe)}",
        timeout=60,
    )
    code = (completed.stdout or "").strip()
    if code == "200":
        report.details["marker_db"] = "present"
        return "present"
    if code == "404":
        report.fail("couchdb init marker missing (controller/invoker will wedge on wait-for-couchdb 404)")
        report.details["marker_db"] = "missing"
        return "missing"
    report.warn(f"couchdb marker probe inconclusive (http={code or 'n/a'})")
    report.details["marker_db"] = "unknown"
    return "unknown"


def count_zombie_pods(args, report: HealthReport) -> int:
    """Count leftover guest/prewarm action pods from a previous run."""
    ns = args.ow_namespace
    completed = _kube_ssh(
        args,
        f"kubectl -n {shlex.quote(ns)} get pods -o name 2>/dev/null | "
        f"grep -E 'wsk{shlex.quote(args.ow_release_name)}-invoker-.*(guest-|prewarm-)' | wc -l",
        timeout=60,
    )
    try:
        n = int((completed.stdout or "0").strip().split()[0])
    except (ValueError, IndexError):
        n = 0
    report.details["zombie_pods"] = n
    if n > 0:
        report.warn(f"{n} leftover guest/prewarm pods from a previous run")
    return n


def check_node_load(args, report: HealthReport) -> dict[str, Any]:
    """Report cluster load. `kubectl top` needs metrics-server, which is absent
    on this cluster, so node %s are best-effort. Foreign-tenant load is reported
    per-namespace for the operator's awareness; it is NOT a hard gate, because
    this is a permanently shared research cluster where neighbours always exist.
    A hard gate only fires if metrics actually show a saturated node."""
    info: dict[str, Any] = {"nodes": {}, "foreign_namespaces": {}}
    report.details["node_load"] = info
    top = _kube_ssh(args, "kubectl top nodes --no-headers", timeout=60)
    if top.returncode == 0 and "Metrics API not available" not in (top.stderr or ""):
        for line in (top.stdout or "").splitlines():
            parts = line.split()
            # NAME CPU(cores) CPU% MEM(bytes) MEM%
            if len(parts) >= 5 and parts[2].endswith("%") and parts[4].endswith("%"):
                try:
                    cpu_pct = int(parts[2].rstrip("%"))
                    mem_pct = int(parts[4].rstrip("%"))
                except ValueError:
                    continue
                info["nodes"][parts[0]] = {"cpu_pct": cpu_pct, "mem_pct": mem_pct}
                if cpu_pct >= _NODE_BUSY_FRACTION * 100 or mem_pct >= _NODE_BUSY_FRACTION * 100:
                    report.warn(f"node {parts[0]} busy at start (cpu {cpu_pct}%, mem {mem_pct}%)")
    else:
        info["metrics_available"] = False  # no hard load gate possible

    # Foreign tenants: Running pods outside our/infra namespaces, summarised by
    # namespace so the warning is actionable rather than a wall of pod names.
    allpods = _kubectl_json(args, "get pods -A")
    if allpods:
        for pod in allpods.get("items", []):
            pns = pod.get("metadata", {}).get("namespace", "")
            if pns in _OWN_NAMESPACES or pns.startswith(_OWN_NAMESPACE_PREFIXES):
                continue
            if pod.get("status", {}).get("phase") == "Running":
                info["foreign_namespaces"][pns] = info["foreign_namespaces"].get(pns, 0) + 1
    if info["foreign_namespaces"]:
        total = sum(info["foreign_namespaces"].values())
        top_ns = sorted(info["foreign_namespaces"].items(), key=lambda kv: -kv[1])[:5]
        report.warn(
            f"{total} foreign-tenant pods across {len(info['foreign_namespaces'])} "
            f"namespaces (busiest: {', '.join(f'{ns}×{c}' for ns, c in top_ns)})"
        )
    return info


# ---------------------------------------------------------------------------
# Remediation
# ---------------------------------------------------------------------------

def reap_guest_pods(args) -> None:
    """Delete leftover guest/prewarm action pods (idempotent)."""
    ns = args.ow_namespace
    rel = args.ow_release_name
    script = (
        f"PODS=$(kubectl -n {shlex.quote(ns)} get pods -o name 2>/dev/null | "
        f"grep -E 'wsk{rel}-invoker-.*(guest-|prewarm-)' || true); "
        f"if [ -n \"$PODS\" ]; then echo \"$PODS\" | "
        f"xargs -r kubectl -n {shlex.quote(ns)} delete --wait=false --ignore-not-found=true; fi"
    )
    print("[preflight] reaping leftover guest/prewarm pods")
    _kube_ssh(args, script, timeout=120)


def rerun_init_couchdb(args, *, wait_timeout: int = 300) -> bool:
    """Clone + re-run the completed ``init-couchdb`` job to recreate the marker
    DB, then bounce the wedged controller/invoker pods so their wait-for-couchdb
    init container re-runs and finds the marker. Returns True on success."""
    ns = args.ow_namespace
    rel = args.ow_release_name
    job = f"{rel}-init-couchdb"
    new_job = f"{rel}-init-couchdb-rerun-{utc_stamp().lower()}"
    # Strip server-populated/immutable fields so the manifest re-applies cleanly.
    clone = (
        f"kubectl -n {shlex.quote(ns)} get job {shlex.quote(job)} -o json | "
        "jq 'del(.spec.selector, .spec.template.metadata.labels, .status, "
        ".metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp, "
        ".metadata.generation, .metadata.ownerReferences) "
        f"| .metadata.name=\"{new_job}\"' | "
        f"kubectl -n {shlex.quote(ns)} apply -f -"
    )
    print(f"[preflight] re-running init-couchdb as {new_job}")
    completed = _kube_ssh(args, clone, timeout=120)
    if completed.returncode != 0:
        print(f"[preflight] init-couchdb clone failed: {completed.stderr.strip()[:400]}")
        return False
    wait = _kube_ssh(
        args,
        f"kubectl -n {shlex.quote(ns)} wait --for=condition=complete "
        f"job/{shlex.quote(new_job)} --timeout={wait_timeout}s",
        timeout=wait_timeout + 30,
    )
    if wait.returncode != 0:
        print(f"[preflight] init-couchdb did not complete: {wait.stderr.strip()[:400]}")
        return False
    # Bounce controller + invoker so wait-for-couchdb re-runs with the marker now present.
    print("[preflight] bouncing controller/invoker to clear wait-for-couchdb wedge")
    _kube_ssh(
        args,
        f"kubectl -n {shlex.quote(ns)} delete pod -l app={shlex.quote(rel)}-controller "
        f"--ignore-not-found=true; "
        f"kubectl -n {shlex.quote(ns)} delete pod -l app={shlex.quote(rel)}-invoker "
        f"--ignore-not-found=true",
        timeout=120,
    )
    return True


def wait_core_ready(args, *, timeout: int = 300, interval: int = 6) -> bool:
    """Poll until core pods are Ready (or timeout)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = HealthReport()
        check_core_pods(args, probe)
        if probe.ok:
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Top-level gate
# ---------------------------------------------------------------------------

def ensure_cluster_ready(
    args,
    *,
    phase: str,
    remediate: bool = True,
) -> HealthReport:
    """Detect cluster health; optionally auto-remediate; gate on failure.

    Raises PreflightError if the cluster is not ready and cannot be remediated.
    Returns the (passing) HealthReport otherwise. Set ``remediate=False`` for a
    pure detect-and-report self-test.
    """
    print(f"[preflight] checking cluster readiness before phase '{phase}'")
    report = HealthReport()
    check_core_pods(args, report)
    marker = check_marker_db(args, report)
    count_zombie_pods(args, report)
    load = check_node_load(args, report)

    # Hard-gate only on *measured* node saturation (requires metrics-server).
    # Foreign tenants merely existing is normal on this shared cluster and is
    # reported as a warning, not a block.
    busy_nodes = [n for n, v in load.get("nodes", {}).items()
                  if v["cpu_pct"] >= _NODE_BUSY_FRACTION * 100
                  or v["mem_pct"] >= _NODE_BUSY_FRACTION * 100]
    if busy_nodes:
        report.fail(
            f"worker node(s) saturated ({', '.join(busy_nodes)}); "
            f"refusing to benchmark on a contended cluster"
        )

    if report.ok:
        print(report.render())
        return report

    if not remediate:
        print(report.render())
        raise PreflightError("cluster not ready (detect-only mode):\n" + report.render())

    print(report.render())
    print("[preflight] attempting auto-remediation")

    # 1. Reap zombie pods (cheap, always safe).
    if report.details.get("zombie_pods", 0) > 0:
        reap_guest_pods(args)

    # 2. Recreate marker DB + bounce control plane if the marker is missing.
    if marker == "missing":
        if not rerun_init_couchdb(args):
            raise PreflightError(
                "couchdb init marker missing and re-init failed; manual recovery "
                "required (check couchdb pod / base DBs may be corrupt):\n" + report.render()
            )

    # 3. Wait for the control plane to settle.
    if not wait_core_ready(args, timeout=getattr(args, "preflight_ready_timeout_sec", 300)):
        post = HealthReport()
        check_core_pods(args, post)
        raise PreflightError(
            "cluster did not reach a ready state after remediation:\n" + post.render()
        )

    # 4. Final verification.
    final = HealthReport()
    check_core_pods(args, final)
    check_marker_db(args, final)
    if not final.ok:
        raise PreflightError("cluster still unhealthy after remediation:\n" + final.render())
    print("[preflight] cluster ready after remediation")
    print(final.render())
    return final
