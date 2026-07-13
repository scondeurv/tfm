#!/usr/bin/env python3
"""Re-run PR rayon cells (V-cost sweep + NUMA placements) after fixing the
parallel pull-style contribute. standalone + mpi unchanged, not re-run."""
import json, subprocess, statistics, time
from pathlib import Path

BIN = "/home/users/sconde/src/pagerank/pagerank-rayon/target/release/pagerank-rayon"
DATA = "/home/users/sconde/datasets"
OUT = Path("/home/users/sconde/extra_datasets/pr_rerun_" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
OUT.mkdir(parents=True, exist_ok=True)
MAX_ITER = 20
REPS = 3

GRAPHS = {
    "soc-LiveJournal1": (f"{DATA}/soc-LiveJournal1.tsv", 4847571),
    "com-orkut": (f"{DATA}/com-orkut.tsv", 3072627),
    "web-Google": (f"{DATA}/web-Google.tsv", 916428),
    "roadNet-CA": (f"{DATA}/roadNet-CA.tsv", 1971281),
}

def run(cmd, timeout=900):
    t0 = time.time()
    p = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=timeout)
    last = None
    for line in p.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try: last = json.loads(line)
            except: pass
    return p.returncode, last, time.time() - t0

VCOST_THREADS = [1, 8, 32, 64]
NUMA = {
    "naive-64": "RAYON_NUM_THREADS=64 {bin} {g} {n} {mi} 64",
    "local-32": "RAYON_NUM_THREADS=32 numactl --cpunodebind=0 --membind=0 {bin} {g} {n} {mi} 32",
    "interleave-64": "RAYON_NUM_THREADS=64 numactl --interleave=all {bin} {g} {n} {mi} 64",
}

results = []
for gname, (gpath, n) in GRAPHS.items():
    print(f"=== {gname} (n={n}) ===", flush=True)
    # V-cost rayon sweep
    for t in VCOST_THREADS:
        times = []
        for rep in range(REPS):
            cmd = f"RAYON_NUM_THREADS={t} {BIN} {gpath} {n} {MAX_ITER} {t}"
            rc, parsed, w = run(cmd)
            ms = parsed.get("execution_time_ms") if parsed else None
            times.append(ms)
            results.append({"graph": gname, "kind": "vcost", "config": f"rayon-{t}", "rep": rep, "ms": ms, "rc": rc})
        med = statistics.median([x for x in times if x is not None]) if any(times) else None
        print(f"  vcost rayon-{t}: {med}", flush=True)
    # NUMA placements
    for cfg, tmpl in NUMA.items():
        times = []
        for rep in range(REPS):
            cmd = tmpl.format(bin=BIN, g=gpath, n=n, mi=MAX_ITER)
            rc, parsed, w = run(cmd)
            ms = parsed.get("execution_time_ms") if parsed else None
            times.append(ms)
            results.append({"graph": gname, "kind": "numa", "config": cfg, "rep": rep, "ms": ms, "rc": rc})
        med = statistics.median([x for x in times if x is not None]) if any(times) else None
        print(f"  numa {cfg}: {med}", flush=True)

(OUT / "pr_rerun.json").write_text(json.dumps(results, indent=2))
print(f"\nDONE -> {OUT}/pr_rerun.json", flush=True)
(OUT / "DONE").write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
