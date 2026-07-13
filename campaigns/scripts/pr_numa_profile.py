#!/usr/bin/env python3
"""Profile PR NUMA placement via numastat deltas + timing breakdown.

Confirms WHY mpi-per-socket dominates: cross-socket memory traffic.

For each config, capture:
  - /sys/devices/system/node/node{0,1}/numastat delta (system-wide)
  - Per-process numa_hit/miss/foreign/local_node/other_node delta (if numastat -p)
  - Timing: build_time_ms, execution_time_ms, total wall
Compares: naive-64 (rayon, no binding) vs local-32 (one socket) vs mpi-per-socket (2 ranks/socket).
Dataset: soc-LiveJournal1.

Output: /tmp/pr_numa_profile.json + stdout summary
"""
from __future__ import annotations
import json
import os
import re
import subprocess
import time
from pathlib import Path

SRC = Path("/home/users/sconde/src/pagerank")
GRAPH = "/home/users/sconde/datasets/soc-LiveJournal1.tsv"
N = 4847571
MAX_ITER = 20
MPI_PREFIX = "/home/users/sconde/opt/openmpi-4.1.5"
REPS = 5
OUT = Path("/home/users/sconde/extra_datasets/pr_numa_profile.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

NUMASTAT_KEYS = ["numa_hit", "numa_miss", "numa_foreign", "interleave_hit", "local_node", "other_node"]


def read_node_numastat(node: int) -> dict[str, int]:
    p = Path(f"/sys/devices/system/node/node{node}/numastat")
    out = {}
    for line in p.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] in NUMASTAT_KEYS:
            out[parts[0]] = int(parts[1])
    return out


def snapshot() -> dict:
    return {n: read_node_numastat(n) for n in [0, 1]}


def delta(a: dict, b: dict) -> dict:
    return {n: {k: b[n][k] - a[n][k] for k in a[n]} for n in a}


def parse_json_last(stdout: str):
    last = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                last = json.loads(line)
            except json.JSONDecodeError:
                pass
    return last


def run_cmd(cmd: str, timeout: int = 600) -> dict:
    pre = snapshot()
    t0 = time.time()
    p = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=timeout)
    wall = time.time() - t0
    post = snapshot()
    parsed = parse_json_last(p.stdout)
    return {
        "rc": p.returncode,
        "wall_s": round(wall, 2),
        "exec_ms": parsed.get("execution_time_ms") if parsed else None,
        "build_ms": parsed.get("build_time_ms") if parsed else None,
        "numa_delta": delta(pre, post),
        "stderr_tail": p.stderr[-200:] if p.returncode != 0 else None,
    }


CONFIGS = {
    "naive-64": f"cd {SRC} && RAYON_NUM_THREADS=64 {SRC}/pagerank-rayon/target/release/pagerank-rayon {GRAPH} {N} {MAX_ITER} 64",
    "local-32": f"cd {SRC} && RAYON_NUM_THREADS=32 numactl --cpunodebind=0 --membind=0 {SRC}/pagerank-rayon/target/release/pagerank-rayon {GRAPH} {N} {MAX_ITER} 32",
    "interleave-64": f"cd {SRC} && RAYON_NUM_THREADS=64 numactl --interleave=all {SRC}/pagerank-rayon/target/release/pagerank-rayon {GRAPH} {N} {MAX_ITER} 64",
    "mpi-per-socket": f"cd {SRC} && {MPI_PREFIX}/bin/mpirun -np 2 --prefix {MPI_PREFIX} --map-by numa --bind-to numa {SRC}/pagerank-mpi/target/release/pagerank-mpi {GRAPH} {N} {MAX_ITER}",
}


results = {}
for cfg, cmd in CONFIGS.items():
    print(f"\n=== {cfg} ===", flush=True)
    rs = []
    for rep in range(REPS):
        print(f"  rep{rep}...", end="", flush=True)
        r = run_cmd(cmd)
        rs.append(r)
        nd = r["numa_delta"]
        # local_node = accesses originated on the same node; other_node = remote
        ln = sum(nd[n].get("local_node", 0) for n in [0, 1])
        on = sum(nd[n].get("other_node", 0) for n in [0, 1])
        ratio = (on / ln) if ln else None
        print(f" rc={r['rc']} exec={r['exec_ms']}ms build={r['build_ms']}ms local={ln} other={on} ratio={ratio:.3f}" if ratio else f" rc={r['rc']} exec={r['exec_ms']}ms")
    results[cfg] = rs

OUT.write_text(json.dumps(results, indent=2))
print(f"\nwrote {OUT}")

# Summary table
print("\n=== SUMMARY (median across reps) ===")
import statistics
print(f"{'config':<20} {'exec_ms':<12} {'build_ms':<12} {'local_node':<14} {'other_node':<14} {'other/local':<12}")
for cfg, rs in results.items():
    em = statistics.median([r["exec_ms"] for r in rs if r["exec_ms"]])
    bm = statistics.median([r["build_ms"] for r in rs if r["build_ms"]])
    lns = [sum(r["numa_delta"][n].get("local_node", 0) for n in [0, 1]) for r in rs]
    ons = [sum(r["numa_delta"][n].get("other_node", 0) for n in [0, 1]) for r in rs]
    ln = statistics.median(lns)
    on = statistics.median(ons)
    ratio = (on / ln) if ln else 0
    print(f"{cfg:<20} {em:<12.0f} {bm:<12.0f} {ln:<14.0f} {on:<14.0f} {ratio:<12.4f}")
