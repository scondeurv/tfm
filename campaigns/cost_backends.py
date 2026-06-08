"""COST-extension backend runners (standalone / rayon / mpi).

These execute on CloudLab compute6/7 via SSH. Each cell produces one raw JSON
record consumed by the unified campaign orchestrator's `cost_sweep` phase.

The runners assume:
  * Rust binaries pre-compiled under `<remote_src_root>/<algo>/<crate>/target/release/`
  * OpenMPI 4.1.5 available on `compute6,compute7` for the MPI runner
  * Graph file staged at `<remote_dataset>` (single TSV, not partitioned)

Each runner returns the dict shape:

    {
        "status": "passed" | "failed",
        "backend": "standalone" | "rayon" | "mpi",
        "raw": {execution_time_ms, total_time_ms, ...} | None,
        "stdout_tail": "...",
        "error": "..." | None,
        # backend-specific extras:
        "threads": <int> (rayon)
        "ranks": <int> (mpi)
        "hosts": "<csv>" (mpi)
        "map_by": "<policy>" (mpi)
    }
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shlex
from pathlib import Path
from typing import Any, Callable, Optional

from cloudlab_common import (
    ssh_command,
)


@dataclasses.dataclass(frozen=True)
class CostBackendConfig:
    """Algorithm-specific layout for COST-extension Rust binaries.

    Paths are relative to the algorithm's working dir (e.g. `labelpropagation/`).
    `cli_tail` returns positional args appended after `<graph_file> <num_nodes>`.
    """
    algorithm: str
    workdir_name: str
    standalone_binary: str
    rayon_binary: str
    mpi_binary: str
    cli_tail: Callable[[argparse.Namespace], list[str]]


COST_BACKEND_CONFIGS: dict[str, CostBackendConfig] = {
    "lp": CostBackendConfig(
        algorithm="lp",
        workdir_name="labelpropagation",
        standalone_binary="lpst/target/release/label-propagation",
        rayon_binary="lp-rayon/target/release/lp-rayon",
        mpi_binary="lp-mpi/target/release/lp-mpi",
        cli_tail=lambda args: [str(args.max_iter)],
    ),
    "bfs": CostBackendConfig(
        algorithm="bfs",
        workdir_name="bfs",
        standalone_binary="bfs-standalone/target/release/bfs-standalone",
        rayon_binary="bfs-rayon/target/release/bfs-rayon",
        mpi_binary="bfs-mpi/target/release/bfs-mpi",
        cli_tail=lambda args: [str(args.source_node), str(args.max_levels)],
    ),
    "sssp": CostBackendConfig(
        algorithm="sssp",
        workdir_name="sssp",
        standalone_binary="sssp-standalone/target/release/sssp-standalone",
        rayon_binary="sssp-rayon/target/release/sssp-rayon",
        mpi_binary="sssp-mpi/target/release/sssp-mpi",
        cli_tail=lambda args: [str(args.source_node), str(args.max_iter)],
    ),
    "pagerank": CostBackendConfig(
        algorithm="pagerank",
        workdir_name="pagerank",
        standalone_binary="pagerank-standalone/target/release/pagerank-standalone",
        rayon_binary="pagerank-rayon/target/release/pagerank-rayon",
        mpi_binary="pagerank-mpi/target/release/pagerank-mpi",
        cli_tail=lambda args: [str(args.max_iter)],
    ),
}


def _remote_algo_dir(args: argparse.Namespace, cfg: CostBackendConfig) -> str:
    return f"{args.cloudlab_src_root}/{cfg.workdir_name}"


def mpi_host_names(args: argparse.Namespace) -> list[str]:
    """Hostnames parsed from --mpi-hosts (e.g. 'compute6:32,compute7:32').

    Drops the slot count after ':' and preserves order. Empty when unset.
    """
    raw = getattr(args, "mpi_hosts", "") or ""
    names: list[str] = []
    for part in raw.split(","):
        host = part.split(":")[0].strip()
        if host and host not in names:
            names.append(host)
    return names


def mpi_extra_hosts(args: argparse.Namespace) -> list[str]:
    """MPI hosts other than the one we SSH into (cloudlab_host).

    These are the nodes that need a local copy of the dataset and binaries
    for cross-host mpirun, because /home is not shared (ext2/3, not NFS).
    Returns [] for single-host MPI (no propagation needed).
    """
    return [h for h in mpi_host_names(args) if h != args.cloudlab_host]


def propagate_remote_file(
    args: argparse.Namespace, remote_path: str, timeout: int = 600,
) -> None:
    """Copy a file already present on cloudlab_host to every extra MPI host,
    same absolute path. No-op when MPI is single-host.

    Runs `mkdir -p` + `scp` *from* cloudlab_host (which has passwordless SSH to
    the other compute nodes) so we never depend on the local machine reaching
    the inner nodes directly.
    """
    extras = mpi_extra_hosts(args)
    if not extras:
        return
    remote_dir = os.path.dirname(remote_path)
    ssh_opts = "-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15"
    for host in extras:
        qhost = shlex.quote(host)
        qpath = shlex.quote(remote_path)
        qdir = shlex.quote(remote_dir)
        # Skip if the peer already has the file with non-zero size (idempotent).
        probe = ssh_command(
            args,
            f"ssh {ssh_opts} {qhost} 'test -s {qpath} && echo OK || echo MISSING'",
            timeout=timeout,
        )
        if probe.returncode == 0 and "MISSING" not in probe.stdout:
            continue
        # scp -p preserves mode/timestamps so an executable binary keeps its
        # +x bit on the peer (datasets are 0644 either way).
        cmd = (
            f"ssh {ssh_opts} {qhost} mkdir -p {qdir} && "
            f"scp -p {ssh_opts} {qpath} {qhost}:{qpath}"
        )
        res = ssh_command(args, cmd, timeout=timeout)
        if res.returncode != 0:
            raise RuntimeError(
                f"failed to propagate {remote_path} to MPI host {host}\n"
                f"STDERR:\n{res.stderr}"
            )


def _parse_benchmark_stdout(stdout: str) -> Optional[dict[str, Any]]:
    """Extract last well-formed top-level JSON object on stdout.

    Rust binaries print free-form progress lines and a single JSON record. We
    parse line-by-line so an unparsable mid-stream message doesn't kill the run.
    """
    candidate = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                pass
    return candidate


def _wrap_result(
    backend: str,
    rc: int,
    stdout: str,
    stderr: str,
    *,
    extras: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    parsed = _parse_benchmark_stdout(stdout)
    result: dict[str, Any] = {
        "status": "passed" if (rc == 0 and parsed is not None) else "failed",
        "backend": backend,
        "raw": parsed,
        "stdout_tail": stdout[-2000:],
        "error": stderr[-500:] if rc != 0 or parsed is None else None,
        "returncode": rc,
    }
    if extras:
        result.update(extras)
    return result


def run_standalone_remote(
    args: argparse.Namespace,
    cfg: CostBackendConfig,
    remote_graph: str,
    nodes: int,
    timeout: int,
) -> dict[str, Any]:
    binary = f"{_remote_algo_dir(args, cfg)}/{cfg.standalone_binary}"
    tail = cfg.cli_tail(args)
    cmd_parts = [
        "cd", shlex.quote(_remote_algo_dir(args, cfg)), "&&",
        shlex.quote(binary), shlex.quote(remote_graph), str(nodes),
    ] + [shlex.quote(t) for t in tail]
    completed = ssh_command(args, " ".join(cmd_parts), timeout=timeout)
    return _wrap_result("standalone", completed.returncode, completed.stdout, completed.stderr)


def run_rayon_remote(
    args: argparse.Namespace,
    cfg: CostBackendConfig,
    remote_graph: str,
    nodes: int,
    threads: int,
    timeout: int,
) -> dict[str, Any]:
    binary = f"{_remote_algo_dir(args, cfg)}/{cfg.rayon_binary}"
    tail = cfg.cli_tail(args)
    env_assignments = f"RAYON_NUM_THREADS={threads}"
    cmd_parts = [
        "cd", shlex.quote(_remote_algo_dir(args, cfg)), "&&",
        env_assignments,
        shlex.quote(binary), shlex.quote(remote_graph), str(nodes),
    ] + [shlex.quote(t) for t in tail] + [str(threads)]
    completed = ssh_command(args, " ".join(cmd_parts), timeout=timeout)
    return _wrap_result(
        "rayon",
        completed.returncode,
        completed.stdout,
        completed.stderr,
        extras={"threads": threads},
    )


def run_mpi_remote(
    args: argparse.Namespace,
    cfg: CostBackendConfig,
    remote_graph: str,
    nodes: int,
    ranks: int,
    hosts: Optional[str],
    timeout: int,
) -> dict[str, Any]:
    binary = f"{_remote_algo_dir(args, cfg)}/{cfg.mpi_binary}"
    tail = cfg.cli_tail(args)
    # If --mpi-prefix given, use the absolute mpirun under it (CloudLab's
    # non-interactive SSH does not source ~/.bashrc, so `mpirun` is not on
    # PATH). Also propagate `--prefix` so mpirun can locate orted on remote ranks.
    if args.mpi_prefix:
        mpirun_bin = f"{args.mpi_prefix}/bin/mpirun"
    else:
        mpirun_bin = "mpirun"
    mpirun_parts: list[str] = [shlex.quote(mpirun_bin), "-np", str(ranks)]
    if args.mpi_prefix:
        mpirun_parts += ["--prefix", shlex.quote(args.mpi_prefix)]
    if args.mpi_btl_if_include:
        mpirun_parts += [
            "--mca", "btl", "tcp,self",
            "--mca", "btl_tcp_if_include", shlex.quote(args.mpi_btl_if_include),
            "--mca", "oob_tcp_if_include", shlex.quote(args.mpi_btl_if_include),
        ]
    if hosts:
        mpirun_parts += ["-H", shlex.quote(hosts)]
    if getattr(args, "mpi_map_by", None):
        mpirun_parts += ["--map-by", shlex.quote(args.mpi_map_by)]
    mpirun_parts += [
        shlex.quote(binary), shlex.quote(remote_graph), str(nodes),
    ] + [shlex.quote(t) for t in tail]
    cmd_parts = [
        "cd", shlex.quote(_remote_algo_dir(args, cfg)), "&&",
    ] + mpirun_parts
    completed = ssh_command(args, " ".join(cmd_parts), timeout=timeout)
    return _wrap_result(
        "mpi",
        completed.returncode,
        completed.stdout,
        completed.stderr,
        extras={"ranks": ranks, "hosts": hosts, "map_by": getattr(args, "mpi_map_by", None)},
    )


def ensure_remote_graph_file(
    args: argparse.Namespace,
    cfg: CostBackendConfig,
    nodes: int,
    local_graph: Path,
    remote_dataset_root: str,
) -> str:
    """Upload single-file graph to CloudLab once per (algo, nodes).

    Path: `<remote_dataset_root>/cost/<algo>/<basename>`. Idempotent: skips
    upload if the remote path already exists with non-zero size.
    """
    from cloudlab_common import scp_to_remote
    remote_dir = f"{remote_dataset_root}/cost/{cfg.algorithm}"
    remote_path = f"{remote_dir}/{local_graph.name}"
    mkdir = ssh_command(args, f"mkdir -p {shlex.quote(remote_dir)}", timeout=60)
    if mkdir.returncode != 0:
        raise RuntimeError(
            f"failed to create remote cost dataset dir {remote_dir}\nSTDERR:\n{mkdir.stderr}"
        )
    probe = ssh_command(
        args,
        f"test -s {shlex.quote(remote_path)} && stat -c %s {shlex.quote(remote_path)} || echo MISSING",
        timeout=60,
    )
    if probe.returncode == 0 and "MISSING" not in probe.stdout:
        return remote_path
    upload = scp_to_remote(args, local_graph, remote_path)
    if upload.returncode != 0:
        raise RuntimeError(
            f"failed to upload graph {local_graph} to {remote_path}\nSTDERR:\n{upload.stderr}"
        )
    return remote_path


def expand_cost_cells(
    *,
    backends: list[str],
    nodes_list: list[int],
    reps: int,
    rayon_threads: list[int],
    mpi_ranks: list[int],
    reps_overrides: dict[int, int] | None = None,
) -> list[dict[str, Any]]:
    """Enumerate every (backend, n, variant, rep) cell to be executed.

    `reps_overrides` is an optional per-n override map (e.g. {10_000_000: 5})
    that replaces the global `reps` for the listed n values. Used to bump
    confidence at the n=10M tail without expanding wall time elsewhere.
    """
    cells: list[dict[str, Any]] = []
    overrides = reps_overrides or {}
    for n in nodes_list:
        n_reps = overrides.get(n, reps)
        if "standalone" in backends:
            for rep in range(n_reps):
                cells.append({"backend": "standalone", "nodes": n, "rep": rep})
        if "rayon" in backends:
            for threads in rayon_threads:
                for rep in range(n_reps):
                    cells.append({"backend": "rayon", "nodes": n, "threads": threads, "rep": rep})
        if "mpi" in backends:
            for ranks in mpi_ranks:
                for rep in range(n_reps):
                    cells.append({"backend": "mpi", "nodes": n, "ranks": ranks, "rep": rep})
    return cells
