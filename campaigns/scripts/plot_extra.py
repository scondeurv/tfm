#!/usr/bin/env python3
"""Generate V-cost + NUMA figures for 3 extra datasets (generalization study)."""
import json
import statistics
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path("experiment_data/extra_datasets/results_20260615T071012Z")
OUT = Path("experiment_data/figures/extra")
OUT.mkdir(parents=True, exist_ok=True)
DATASETS = [("com-orkut", "com-orkut\n(3M nodes, 117M edges, social)"),
            ("web-Google", "web-Google\n(916k nodes, 5.1M edges, web)"),
            ("roadNet-CA", "roadNet-CA\n(1.97M nodes, 5.5M edges, road)")]
ALGOS = ["lp", "bfs", "sssp", "pagerank"]
ALGO_LABEL = {"lp": "LP (20 iter)", "bfs": "BFS", "sssp": "SSSP", "pagerank": "PageRank (20 iter)"}


def med(vs):
    return statistics.median(vs) if vs else None


def load(p):
    return [json.loads(l) for l in open(p)]


# Aggregate
agg = {}
for ds, _ in DATASETS:
    vc = load(ROOT / ds / "vcost.jsonl")
    nu = load(ROOT / ds / "numa.jsonl")
    for algo in ALGOS:
        cs = [c for c in vc if c["algo"] == algo and c["status"] == "passed"]
        agg[(ds, algo, "standalone", None)] = med([c["execution_time_ms"] for c in cs if c["backend"] == "standalone"])
        for t in [1, 8, 32, 64]:
            agg[(ds, algo, "rayon", t)] = med([c["execution_time_ms"] for c in cs if c["backend"] == "rayon" and c.get("threads") == t])
        for r in [2, 4, 8, 16]:
            agg[(ds, algo, "mpi", r)] = med([c["execution_time_ms"] for c in cs if c["backend"] == "mpi" and c.get("ranks") == r])
        cs = [c for c in nu if c["algo"] == algo and c["status"] == "passed"]
        for cfg in ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]:
            agg[(ds, algo, "numa", cfg)] = med([c["execution_time_ms"] for c in cs if c["config"] == cfg])


# ---- 1. V-cost scaling per algo (3 panels = 3 datasets) ----
for algo in ALGOS:
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), sharey=False)
    for ax, (ds, ds_lab) in zip(axes, DATASETS):
        ray_pts = sorted([(t, agg[(ds, algo, "rayon", t)]) for t in [1, 8, 32, 64] if agg.get((ds, algo, "rayon", t))])
        mpi_pts = sorted([(r, agg[(ds, algo, "mpi", r)]) for r in [2, 4, 8, 16] if agg.get((ds, algo, "mpi", r))])
        sa = agg.get((ds, algo, "standalone", None))
        if ray_pts:
            ts, vs = zip(*ray_pts)
            ax.plot(ts, vs, "o-", label="rayon", color="C0", linewidth=2, markersize=7)
        if mpi_pts:
            rs, vs = zip(*mpi_pts)
            ax.plot(rs, vs, "s-", label="MPI (1 host)", color="C1", linewidth=2, markersize=7)
        if sa:
            ax.axhline(sa, color="C2", linestyle="--", label=f"standalone={sa:.0f}ms")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("Resources")
        ax.set_ylabel("ms")
        ax.set_title(ds_lab, fontsize=10)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=8)
    fig.suptitle(f"V-cost scaling — {ALGO_LABEL[algo]}", fontweight="bold")
    fig.tight_layout()
    p = OUT / f"extra_vcost_{algo}.png"
    fig.savefig(p, dpi=120)
    plt.close(fig)
    print(f"wrote {p}")


# ---- 2. NUMA grouped bar (1 panel per dataset, 4 cfgs × 4 algos) ----
configs = ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]
fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=False)
for ax, (ds, ds_lab) in zip(axes, DATASETS):
    x = list(range(len(ALGOS)))
    w = 0.2
    colors = ["C0", "C2", "C3", "C4"]
    for i, cfg in enumerate(configs):
        vals = [agg.get((ds, a, "numa", cfg)) or 0 for a in ALGOS]
        ax.bar([xi + (i - 1.5) * w for xi in x], vals, w, label=cfg, color=colors[i])
    ax.set_xticks(x)
    ax.set_xticklabels([ALGO_LABEL[a].split(" ")[0] for a in ALGOS])
    ax.set_ylabel("Median ms")
    ax.set_yscale("log")
    ax.set_title(ds_lab, fontsize=10)
    ax.legend(title="Placement", fontsize=8, loc="best")
    ax.grid(True, axis="y", alpha=0.3)
fig.suptitle("NUMA placement — compute6 (2× EPYC 7502)", fontweight="bold")
fig.tight_layout()
p = OUT / "extra_numa.png"
fig.savefig(p, dpi=120)
plt.close(fig)
print(f"wrote {p}")


# ---- 3. COST summary across datasets ----
fig, ax = plt.subplots(figsize=(12, 6))
x = list(range(len(ALGOS)))
w = 0.25
for i, (ds, ds_lab) in enumerate(DATASETS):
    cost_vals = []
    for algo in ALGOS:
        vert_cands = [agg.get((ds, algo, "standalone", None))]
        for t in [1, 8, 32, 64]:
            vert_cands.append(agg.get((ds, algo, "rayon", t)))
        for cfg in ["naive-64", "local-32", "interleave-64"]:
            vert_cands.append(agg.get((ds, algo, "numa", cfg)))
        vert_cands = [v for v in vert_cands if v]
        bv = min(vert_cands)
        mpis = sorted([(r, agg.get((ds, algo, "mpi", r))) for r in [2, 4, 8, 16] if agg.get((ds, algo, "mpi", r))])
        cost = next((r for r, v in mpis if v <= bv * 1.05), None)  # 5% match tol (±5% CV)
        cost_vals.append(cost if cost else 32)  # 32 = "∞" marker for plotting
    bars = ax.bar([xi + (i - 1) * w for xi in x], cost_vals, w, label=ds, color=f"C{i}")
    for xi, v in zip(x, cost_vals):
        if v == 32:
            ax.text(xi + (i - 1) * w, 32, "∞", ha="center", va="bottom", fontweight="bold", fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels([ALGO_LABEL[a].split(" ")[0] for a in ALGOS])
ax.set_ylabel("COST: ranks to match best vertical (∞=unreached)")
ax.set_title("Cross-dataset COST: vertical-vs-horizontal threshold", fontweight="bold")
ax.legend()
ax.grid(True, axis="y", alpha=0.3)
ax.set_yticks([0, 2, 4, 8, 16, 32])
ax.set_yticklabels(["0", "2", "4", "8", "16", "∞"])
fig.tight_layout()
p = OUT / "extra_cost_summary.png"
fig.savefig(p, dpi=120)
plt.close(fig)
print(f"wrote {p}")

print("\nALL FIGURES DONE")
