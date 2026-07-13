#!/usr/bin/env python3
"""Plot PR NUMA profile: exec time + numastat alloc placement.

Bar chart: exec time per config. Second panel: numastat local vs other (allocations).
Annotates ratio + speedup.
"""
import json
import statistics
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

data = json.load(open("experiment_data/extra_datasets/pr_numa_profile.json"))
CFGS = ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]
COLORS = {"naive-64": "C0", "local-32": "C2", "interleave-64": "C3", "mpi-per-socket": "C4"}

agg = {}
for cfg, rs in data.items():
    em = statistics.median([r["exec_ms"] for r in rs if r["exec_ms"]])
    bm = statistics.median([r["build_ms"] for r in rs if r["build_ms"]])
    lns = [sum(r["numa_delta"][n].get("local_node", 0) for n in ["0", "1"]) for r in rs]
    ons = [sum(r["numa_delta"][n].get("other_node", 0) for n in ["0", "1"]) for r in rs]
    ln = statistics.median(lns)
    on = statistics.median(ons)
    agg[cfg] = {"exec_ms": em, "build_ms": bm, "local": ln, "other": on, "ratio": on / ln if ln else 0}

baseline = agg["naive-64"]["exec_ms"]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

# Panel 1: exec time
x = list(range(len(CFGS)))
vals = [agg[c]["exec_ms"] for c in CFGS]
bars = ax1.bar(x, vals, color=[COLORS[c] for c in CFGS], edgecolor="black")
ax1.set_xticks(x)
ax1.set_xticklabels(CFGS, rotation=15, ha="right")
ax1.set_ylabel("PageRank execution time (ms)")
ax1.set_title("PR (soc-LiveJournal1, n=4.85M, 20 iter) — n=5 reps median", pad=24)
ax1.grid(True, axis="y", alpha=0.3)
ax1.set_ylim(0, max(vals) * 1.20)  # headroom so bar labels clear the title
for xi, v, c in zip(x, vals, CFGS):
    speed = baseline / v
    ax1.text(xi, v + max(vals) * 0.015, f"{v:.0f}ms\n{speed:.2f}×",
             ha="center", va="bottom", fontweight="bold", fontsize=10)

# Panel 2: NUMA alloc placement
local_vals = [agg[c]["local"] for c in CFGS]
other_vals = [agg[c]["other"] for c in CFGS]
w = 0.35
ax2.bar([xi - w/2 for xi in x], local_vals, w, label="local_node alloc", color="C2", edgecolor="black")
ax2.bar([xi + w/2 for xi in x], other_vals, w, label="other_node alloc (remote)", color="C3", edgecolor="black")
ax2.set_xticks(x)
ax2.set_xticklabels(CFGS, rotation=15, ha="right")
ax2.set_ylabel("numastat alloc events (delta)")
ax2.set_title("NUMA allocation placement (numastat delta)")
ax2.legend()
ax2.grid(True, axis="y", alpha=0.3)
for xi, c in zip(x, CFGS):
    r = agg[c]["ratio"]
    if r > 0.01:
        ax2.text(xi, max(agg[c]["local"], agg[c]["other"]) * 1.05, f"r/l={r:.2f}",
                 ha="center", fontweight="bold", color="C3", fontsize=10)

fig.suptitle("PR NUMA placement profile — compute6 (2× EPYC 7502, 2 sockets)", fontweight="bold")
fig.tight_layout()
for out in [Path("experiment_data/figures/extra/pr_numa_profile.png"),
            Path("doc-tfm/memoria/figures/pr_numa_profile.png")]:
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120)
    print(f"wrote {out}")
