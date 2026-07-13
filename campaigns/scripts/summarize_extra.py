#!/usr/bin/env python3
"""Summarize extra datasets V-cost + NUMA results into tables."""
import json
import statistics
from pathlib import Path

ROOT = Path("experiment_data/extra_datasets/results_20260615T071012Z")
DATASETS = ["com-orkut", "web-Google", "roadNet-CA"]
ALGOS = ["lp", "bfs", "sssp", "pagerank"]


def load_jsonl(p):
    return [json.loads(line) for line in open(p)]


def med(vs):
    return statistics.median(vs) if vs else None


for ds in DATASETS:
    meta = json.load(open(ROOT / ds / "meta.json"))
    vc = load_jsonl(ROOT / ds / "vcost.jsonl")
    nu = load_jsonl(ROOT / ds / "numa.jsonl")
    print(f"\n========== {ds} (n={meta['n']:,} source={meta['source']}) ==========")
    print("--- V-COST (median ms) ---")
    print(f"{'Algo':<10} {'Stand':<8} {'r-1':<8} {'r-8':<8} {'r-32':<8} {'r-64':<8} {'mpi-2':<8} {'mpi-4':<8} {'mpi-8':<8} {'mpi-16':<8}")
    for algo in ALGOS:
        cells = [c for c in vc if c["algo"] == algo and c["status"] == "passed"]
        sa = med([c["execution_time_ms"] for c in cells if c["backend"] == "standalone"])
        rays = {t: med([c["execution_time_ms"] for c in cells if c["backend"] == "rayon" and c.get("threads") == t]) for t in [1, 8, 32, 64]}
        mpis = {r: med([c["execution_time_ms"] for c in cells if c["backend"] == "mpi" and c.get("ranks") == r]) for r in [2, 4, 8, 16]}
        row = [f"{sa:.0f}" if sa else "-"]
        row += [f"{rays[t]:.0f}" if rays[t] else "-" for t in [1, 8, 32, 64]]
        row += [f"{mpis[r]:.0f}" if mpis[r] else "-" for r in [2, 4, 8, 16]]
        print(f"{algo:<10} " + " ".join(f"{v:<8}" for v in row))

    print("--- NUMA (median ms) ---")
    print(f"{'Algo':<10} {'naive-64':<12} {'local-32':<12} {'interleave-64':<14} {'mpi-per-sock':<14}")
    for algo in ALGOS:
        cells = [c for c in nu if c["algo"] == algo and c["status"] == "passed"]
        vals = {cfg: med([c["execution_time_ms"] for c in cells if c["config"] == cfg]) for cfg in ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]}
        row = [f"{vals[cfg]:.0f}" if vals[cfg] else "-" for cfg in ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]]
        print(f"{algo:<10} {row[0]:<12} {row[1]:<12} {row[2]:<14} {row[3]:<14}")

    print("--- COST (ranks to match best vertical) ---")
    for algo in ALGOS:
        vc_cells = [c for c in vc if c["algo"] == algo and c["status"] == "passed"]
        nu_cells = [c for c in nu if c["algo"] == algo and c["status"] == "passed"]
        sa = med([c["execution_time_ms"] for c in vc_cells if c["backend"] == "standalone"])
        rays = [(t, med([c["execution_time_ms"] for c in vc_cells if c["backend"] == "rayon" and c.get("threads") == t])) for t in [1, 8, 32, 64]]
        numas = [(cfg, med([c["execution_time_ms"] for c in nu_cells if c["config"] == cfg])) for cfg in ["naive-64", "local-32", "interleave-64"]]
        verts = [("standalone", sa)] + [(f"rayon-{t}", v) for t, v in rays if v] + [(f"numa-{cfg}", v) for cfg, v in numas if v]
        bv = min(verts, key=lambda x: x[1])
        mpis = sorted([(r, med([c["execution_time_ms"] for c in vc_cells if c["backend"] == "mpi" and c.get("ranks") == r])) for r in [2, 4, 8, 16]])
        cost = next((r for r, v in mpis if v and v <= bv[1]), None)
        bm = min((p for p in mpis if p[1]), key=lambda x: x[1])
        print(f"{algo}: best-vert={bv[0]}={bv[1]:.0f}ms  best-mpi=r{bm[0]}={bm[1]:.0f}ms  COST={cost if cost else 'unreached'}")
