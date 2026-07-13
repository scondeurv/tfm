#!/usr/bin/env python3
"""Re-run LP standalone + rayon sweep + NUMA after the sort-based majority
optimisation (HashMap -> sorted scratch). MPI unchanged. Args:
  standalone: <graph> <n> <max_iter>
  rayon:      <graph> <n> <max_iter> [threads]"""
import json, subprocess, statistics, time
from pathlib import Path

SA = "/home/users/sconde/src/labelpropagation/lpst/target/release/label-propagation"
RAY = "/home/users/sconde/src/labelpropagation/lp-rayon/target/release/lp-rayon"
DATA = "/home/users/sconde/datasets"
OUT = Path("/home/users/sconde/extra_datasets/lp_rerun_" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
OUT.mkdir(parents=True, exist_ok=True)
MAX_ITER = 20
REPS = 3
GRAPHS = {
    "soc-LiveJournal1": (f"{DATA}/soc-LiveJournal1.tsv", 4847571),
    "com-orkut":        (f"{DATA}/com-orkut.tsv", 3072627),
    "web-Google":       (f"{DATA}/web-Google.tsv", 916428),
    "roadNet-CA":       (f"{DATA}/roadNet-CA.tsv", 1971281),
}

def run(cmd, timeout=1200):
    p = subprocess.run(["bash","-lc",cmd], capture_output=True, text=True, timeout=timeout)
    last=None
    for ln in p.stdout.splitlines():
        ln=ln.strip()
        if ln.startswith("{") and ln.endswith("}"):
            try: last=json.loads(ln)
            except: pass
    return p.returncode, last

VC=[1,8,32,64]
NUMA={
 "naive-64":"RAYON_NUM_THREADS=64 {b} {g} {n} {mi} 64",
 "local-32":"RAYON_NUM_THREADS=32 numactl --cpunodebind=0 --membind=0 {b} {g} {n} {mi} 32",
 "interleave-64":"RAYON_NUM_THREADS=64 numactl --interleave=all {b} {g} {n} {mi} 64",
}
res=[]
for gname,(g,n) in GRAPHS.items():
    print(f"=== {gname} (n={n}) ===", flush=True)
    # standalone
    t=[]
    for rep in range(REPS):
        rc,parsed=run(f"{SA} {g} {n} {MAX_ITER}")
        ms=parsed.get("execution_time_ms") if parsed else None
        t.append(ms); res.append({"graph":gname,"kind":"standalone","config":"standalone","rep":rep,"ms":ms,"rc":rc})
    print(f"  standalone: {statistics.median([x for x in t if x])}", flush=True)
    for th in VC:
        t=[]
        for rep in range(REPS):
            rc,parsed=run(f"RAYON_NUM_THREADS={th} {RAY} {g} {n} {MAX_ITER} {th}")
            ms=parsed.get("execution_time_ms") if parsed else None
            t.append(ms); res.append({"graph":gname,"kind":"vcost","config":f"rayon-{th}","rep":rep,"ms":ms,"rc":rc})
        print(f"  rayon-{th}: {statistics.median([x for x in t if x])}", flush=True)
    for cfg,tmpl in NUMA.items():
        t=[]
        for rep in range(REPS):
            rc,parsed=run(tmpl.format(b=RAY,g=g,n=n,mi=MAX_ITER))
            ms=parsed.get("execution_time_ms") if parsed else None
            t.append(ms); res.append({"graph":gname,"kind":"numa","config":cfg,"rep":rep,"ms":ms,"rc":rc})
        print(f"  numa {cfg}: {statistics.median([x for x in t if x])}", flush=True)

(OUT/"lp_rerun.json").write_text(json.dumps(res,indent=2))
(OUT/"DONE").write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
print(f"DONE -> {OUT}/lp_rerun.json", flush=True)
