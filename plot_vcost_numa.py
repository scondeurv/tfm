#!/usr/bin/env python3
"""Generate V-cost + NUMA figures for TFM.

Outputs to experiment_data/figures/:
  vcost_scaling_{algo}.png  : standalone (h-line) + rayon (line vs threads) + mpi (line vs ranks)
  numa_placement.png        : 4 algos x 4 configs grouped bar chart
  cost_summary.png          : bar of best-vertical vs best-MPI per algo + COST annotation
"""
import json
import statistics
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

VCOST = Path("experiment_data/cloudlab_campaigns/vcost-20260614T111222Z/cost_sweep")
NUMA = Path("experiment_data/numa/numa_results.json")
OUT = Path("experiment_data/figures")
OUT.mkdir(parents=True, exist_ok=True)

ALGOS = ["lp", "bfs", "sssp", "pagerank"]
ALGO_LABEL = {"lp": "LP (20 iter)", "bfs": "BFS", "sssp": "SSSP", "pagerank": "PageRank (20 iter)"}


def med(vs):
    return statistics.median(vs) if vs else None


def load_backend(name, key):
    rows = json.load(open(VCOST / f"runs_{name}.json"))
    g = {}
    for r in rows:
        if r["status"] != "passed":
            continue
        v = r["result"]["raw"].get("execution_time_ms")
        if v is None:
            continue
        k = (r["algorithm"], r[key]) if key else r["algorithm"]
        g.setdefault(k, []).append(v)
    return {k: med(vs) for k, vs in g.items()}


sa = load_backend("standalone", None)
ray = load_backend("rayon", "threads")
mpi = load_backend("mpi", "ranks")
numa = json.load(open(NUMA))

numa_med = {}
for r in numa:
    if r["status"] != "passed":
        continue
    numa_med.setdefault((r["algo"], r["config"]), []).append(r["execution_time_ms"])
numa_med = {k: med(vs) for k, vs in numa_med.items()}

# ---------- 1. V-cost scaling per algo ----------
for algo in ALGOS:
    fig, ax = plt.subplots(figsize=(8, 5))
    ray_pts = sorted([(t, v) for (a, t), v in ray.items() if a == algo])
    mpi_pts = sorted([(r, v) for (a, r), v in mpi.items() if a == algo])
    if ray_pts:
        ts, vs = zip(*ray_pts)
        ax.plot(ts, vs, "o-", label="rayon (1 node, threads)", color="C0", linewidth=2, markersize=8)
    if mpi_pts:
        rs, vs = zip(*mpi_pts)
        ax.plot(rs, vs, "s-", label="MPI (cross-host, ranks)", color="C1", linewidth=2, markersize=8)
    if algo in sa:
        ax.axhline(sa[algo], color="C2", linestyle="--", label=f"standalone (1 thread) = {sa[algo]:.0f}ms")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Resources (threads or ranks)")
    ax.set_ylabel("Execution time (ms)")
    ax.set_title(f"V-cost scaling — {ALGO_LABEL[algo]} (soc-LiveJournal1, n=4.85M)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    p = OUT / f"vcost_scaling_{algo}.png"
    fig.savefig(p, dpi=120)
    plt.close(fig)
    print(f"wrote {p}")

# ---------- 2. NUMA placement grouped bar ----------
configs = ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]
fig, ax = plt.subplots(figsize=(10, 6))
x = list(range(len(ALGOS)))
w = 0.2
colors = ["C0", "C2", "C3", "C4"]
for i, cfg in enumerate(configs):
    vals = [numa_med.get((a, cfg), 0) for a in ALGOS]
    ax.bar([xi + (i - 1.5) * w for xi in x], vals, w, label=cfg, color=colors[i])
ax.set_xticks(x)
ax.set_xticklabels([ALGO_LABEL[a] for a in ALGOS])
ax.set_ylabel("Median execution time (ms)")
ax.set_yscale("log")
ax.set_title("NUMA placement — compute6 (2× EPYC 7502, 2 NUMA nodes)")
ax.legend(title="Placement", loc="upper left")
ax.grid(True, axis="y", alpha=0.3)
fig.tight_layout()
p = OUT / "numa_placement.png"
fig.savefig(p, dpi=120)
plt.close(fig)
print(f"wrote {p}")

# ---------- 3. COST summary ----------
fig, ax = plt.subplots(figsize=(10, 6))
labels = []
vert_vals = []
mpi_vals = []
cost_lab = []
for algo in ALGOS:
    cands = []
    if algo in sa:
        cands.append((sa[algo], "standalone"))
    for (a, t), v in ray.items():
        if a == algo:
            cands.append((v, f"rayon-{t}"))
    for (a, c), v in numa_med.items():
        if a == algo:
            cands.append((v, f"numa-{c}"))
    bv, bcfg = min(cands)
    mpi_algo = sorted([(t, v) for (a, t), v in mpi.items() if a == algo])
    bmr, bmv = min(mpi_algo, key=lambda x: x[1])
    cost = next((r for r, v in mpi_algo if v <= bv), None)
    labels.append(ALGO_LABEL[algo])
    vert_vals.append(bv)
    mpi_vals.append(bmv)
    cost_lab.append(f"COST={cost}" if cost else "COST=∞")

x = list(range(len(labels)))
ax.bar([xi - 0.2 for xi in x], vert_vals, 0.4, label="Best vertical (1 node)", color="C2")
ax.bar([xi + 0.2 for xi in x], mpi_vals, 0.4, label="Best MPI (cross-host)", color="C1")
for xi, lab in zip(x, cost_lab):
    ymax = max(vert_vals[xi], mpi_vals[xi])
    ax.text(xi, ymax * 1.1, lab, ha="center", fontweight="bold")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_ylabel("Median execution time (ms)")
ax.set_yscale("log")
ax.set_title("COST: horizontal ranks needed to match best vertical (soc-LiveJournal1)")
ax.legend()
ax.grid(True, axis="y", alpha=0.3)
fig.tight_layout()
p = OUT / "cost_summary.png"
fig.savefig(p, dpi=120)
plt.close(fig)
print(f"wrote {p}")

print("\nALL FIGURES DONE")
