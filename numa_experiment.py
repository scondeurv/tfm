#!/usr/bin/env python3
"""NUMA locality experiment on compute6 (2x AMD EPYC 7502, 2 NUMA nodes).

Compares placement policies for the COST Rust backends on a real graph:
  naive-64      : rayon, 64 threads, no pinning (OS spreads across both sockets)
  local-32      : numactl --cpunodebind=0 --membind=0, rayon 32 threads (1 socket, local mem)
  interleave-64 : numactl --interleave=all, rayon 64 threads (full node, balanced mem)
  mpi-per-socket: mpirun --map-by numa --bind-to numa -np 2 (1 rank/socket, NUMA-local)

Vertical baseline question: does NUMA-aware placement beat naive shared-memory
scaling on a dual-socket node? Runs entirely on compute6 (single node).
"""
from __future__ import annotations
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

import os
HOST = "compute6"
SRC = "/home/users/sconde/src"
PLAIN = os.environ.get("GRAPH_PLAIN", "/home/users/sconde/datasets/soc-LiveJournal1.tsv")
WEIGHTED = os.environ.get("GRAPH_WEIGHTED", "/home/users/sconde/datasets/soc-LiveJournal1-weighted.tsv")
N = int(os.environ.get("NUM_NODES", "4847571"))
MAX_ITER = 20
SOURCE = 0
MAX_LEVELS = 500
REPS = 5
MPI_PREFIX = "/home/users/sconde/opt/openmpi-4.1.5"

ALGOS = {
    # algo: (dir, rayon_bin, mpi_bin, graph, tail_args)
    "lp": ("labelpropagation", "lp-rayon/target/release/lp-rayon",
           "lp-mpi/target/release/lp-mpi", PLAIN, [str(MAX_ITER)]),
    "bfs": ("bfs", "bfs-rayon/target/release/bfs-rayon",
            "bfs-mpi/target/release/bfs-mpi", PLAIN, [str(SOURCE), str(MAX_LEVELS)]),
    "sssp": ("sssp", "sssp-rayon/target/release/sssp-rayon",
             "sssp-mpi/target/release/sssp-mpi", WEIGHTED, [str(SOURCE), str(MAX_ITER)]),
    "pagerank": ("pagerank", "pagerank-rayon/target/release/pagerank-rayon",
                 "pagerank-mpi/target/release/pagerank-mpi", PLAIN, [str(MAX_ITER)]),
}


def ssh_run(remote_cmd: str, timeout: int = 1800) -> tuple[int, str, str]:
    p = subprocess.run(
        ["ssh", "-F", "/home/sergio/.ssh/config", HOST, remote_cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    return p.returncode, p.stdout, p.stderr


def parse_ms(stdout: str):
    cand = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                cand = json.loads(line)
            except json.JSONDecodeError:
                pass
    if cand:
        return cand.get("execution_time_ms")
    return None


def build_cmd(algo: str, config: str) -> str:
    d, rayon_bin, mpi_bin, graph, tail = ALGOS[algo]
    wd = f"{SRC}/{d}"
    tail_s = " ".join(tail)
    if config == "naive-64":
        return (f"cd {wd} && RAYON_NUM_THREADS=64 "
                f"{wd}/{rayon_bin} {graph} {N} {tail_s} 64")
    if config == "local-32":
        return (f"cd {wd} && RAYON_NUM_THREADS=32 numactl --cpunodebind=0 --membind=0 "
                f"{wd}/{rayon_bin} {graph} {N} {tail_s} 32")
    if config == "interleave-64":
        return (f"cd {wd} && RAYON_NUM_THREADS=64 numactl --interleave=all "
                f"{wd}/{rayon_bin} {graph} {N} {tail_s} 64")
    if config == "mpi-per-socket":
        return (f"cd {wd} && {MPI_PREFIX}/bin/mpirun -np 2 --prefix {MPI_PREFIX} "
                f"--map-by numa --bind-to numa "
                f"{wd}/{mpi_bin} {graph} {N} {tail_s}")
    raise ValueError(config)


CONFIGS = ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]


def main():
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("experiment_data/numa_results.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    results = []
    for algo in ALGOS:
        for config in CONFIGS:
            times = []
            for rep in range(REPS):
                cmd = build_cmd(algo, config)
                t0 = time.time()
                rc, so, se = ssh_run(cmd)
                wall = time.time() - t0
                ms = parse_ms(so)
                status = "passed" if rc == 0 and ms is not None else "failed"
                rec = {"algo": algo, "config": config, "rep": rep,
                       "status": status, "execution_time_ms": ms,
                       "wall_s": round(wall, 1),
                       "error": (se[-300:] if status == "failed" else None)}
                results.append(rec)
                print(f"[{algo}/{config}] rep{rep} {status} ms={ms} wall={wall:.1f}s", flush=True)
                if status == "failed":
                    print(f"    ERR: {se[-300:]}", flush=True)
            ok = [r["execution_time_ms"] for r in results
                  if r["algo"] == algo and r["config"] == config and r["status"] == "passed"]
            if ok:
                med = statistics.median(ok)
                cv = (statistics.pstdev(ok) / med * 100) if len(ok) > 1 and med else 0
                print(f"  => {algo}/{config} median={med:.0f}ms CV={cv:.1f}% n={len(ok)}", flush=True)
        out.write_text(json.dumps(results, indent=2))
    print(f"\nWROTE {out}", flush=True)


if __name__ == "__main__":
    main()
