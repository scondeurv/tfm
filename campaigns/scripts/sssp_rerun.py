#!/usr/bin/env python3
"""Re-run SSSP rayon cells (V-cost sweep + NUMA placements) after the
load-guard + parallel-prev-copy optimisation. standalone + MPI unchanged."""
import json, subprocess, statistics, time
from pathlib import Path

BIN = "/home/users/sconde/src/sssp/sssp-rayon/target/release/sssp-rayon"
DATA = "/home/users/sconde/datasets"
OUT = Path("/home/users/sconde/extra_datasets/sssp_rerun_" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
OUT.mkdir(parents=True, exist_ok=True)
MAX_ITER = 20
REPS = 3

# (weighted graph path, n, source)
GRAPHS = {
    "soc-LiveJournal1": (f"{DATA}/soc-LiveJournal1-weighted.tsv", 4847571, 0),
    "com-orkut":        (f"{DATA}/com-orkut-weighted.tsv", 3072627, 43608),
    "web-Google":       (f"{DATA}/web-Google-weighted.tsv", 916428, 506742),
    "roadNet-CA":       (f"{DATA}/roadNet-CA-weighted.tsv", 1971281, 562818),
}

def run(cmd, timeout=900):
    p = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=timeout)
    last = None
    for line in p.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try: last = json.loads(line)
            except: pass
    return p.returncode, last

VCOST_THREADS = [1, 8, 32, 64]
NUMA = {
    "naive-64": "RAYON_NUM_THREADS=64 {bin} {g} {n} {s} {mi} 64",
    "local-32": "RAYON_NUM_THREADS=32 numactl --cpunodebind=0 --membind=0 {bin} {g} {n} {s} {mi} 32",
    "interleave-64": "RAYON_NUM_THREADS=64 numactl --interleave=all {bin} {g} {n} {s} {mi} 64",
}

results = []
for gname, (gpath, n, src) in GRAPHS.items():
    print(f"=== {gname} (n={n} src={src}) ===", flush=True)
    for t in VCOST_THREADS:
        times = []
        for rep in range(REPS):
            rc, parsed = run(f"RAYON_NUM_THREADS={t} {BIN} {gpath} {n} {src} {MAX_ITER} {t}")
            ms = parsed.get("execution_time_ms") if parsed else None
            reach = parsed.get("reachable_nodes") if parsed else None
            times.append(ms)
            results.append({"graph": gname, "kind": "vcost", "config": f"rayon-{t}", "rep": rep, "ms": ms, "reachable": reach, "rc": rc})
        m = statistics.median([x for x in times if x is not None]) if any(times) else None
        print(f"  vcost rayon-{t}: {m} (reach={results[-1]['reachable']})", flush=True)
    for cfg, tmpl in NUMA.items():
        times = []
        for rep in range(REPS):
            rc, parsed = run(tmpl.format(bin=BIN, g=gpath, n=n, s=src, mi=MAX_ITER))
            ms = parsed.get("execution_time_ms") if parsed else None
            times.append(ms)
            results.append({"graph": gname, "kind": "numa", "config": cfg, "rep": rep, "ms": ms, "rc": rc})
        m = statistics.median([x for x in times if x is not None]) if any(times) else None
        print(f"  numa {cfg}: {m}", flush=True)

(OUT / "sssp_rerun.json").write_text(json.dumps(results, indent=2))
(OUT / "DONE").write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
print(f"DONE -> {OUT}/sssp_rerun.json", flush=True)
