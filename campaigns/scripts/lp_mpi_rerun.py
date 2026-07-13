#!/usr/bin/env python3
"""Re-run LP MPI cells (V-cost sweep) after sorted-mode majority fix
(labelpropagation@2cebbb4). standalone + rayon unchanged."""
import json, subprocess, statistics, time
from pathlib import Path

BIN = "/home/users/sconde/src/labelpropagation/lp-mpi/target/release/lp-mpi"
MPIRUN = "/home/users/sconde/opt/openmpi-4.1.5/bin/mpirun"
DATA = "/home/users/sconde/datasets"
OUT = Path("/home/users/sconde/extra_datasets/lp_mpi_rerun_" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
OUT.mkdir(parents=True, exist_ok=True)
MAX_ITER = 20
REPS = 3

GRAPHS = {
    "soc-LiveJournal1": (f"{DATA}/soc-LiveJournal1.tsv", 4847571),
    "com-orkut":        (f"{DATA}/com-orkut.tsv", 3072627),
    "web-Google":       (f"{DATA}/web-Google.tsv", 916428),
    "roadNet-CA":       (f"{DATA}/roadNet-CA.tsv", 1971281),
}
RANKS = [2, 4, 8, 16, 32]

ENV = ("export PATH=/home/users/sconde/opt/openmpi-4.1.5/bin:$PATH; "
       "export LD_LIBRARY_PATH=/home/users/sconde/opt/openmpi-4.1.5/lib; ")

def run(cmd, timeout=1800):
    p = subprocess.run(["bash","-lc", ENV + cmd], capture_output=True, text=True, timeout=timeout)
    last=None
    for ln in p.stdout.splitlines():
        ln=ln.strip()
        if ln.startswith("{") and ln.endswith("}"):
            try: last=json.loads(ln)
            except: pass
    return p.returncode, last, p.stderr[-400:] if p.stderr else ""

results=[]
for gname,(g,n) in GRAPHS.items():
    print(f"=== {gname} (n={n}) ===", flush=True)
    for r in RANKS:
        times=[]
        for rep in range(REPS):
            cmd = f"{MPIRUN} -n {r} --oversubscribe {BIN} {g} {n} {MAX_ITER}"
            rc, parsed, err = run(cmd)
            ms = parsed.get("execution_time_ms") if parsed else None
            times.append(ms)
            results.append({"graph":gname,"kind":"vcost","backend":"mpi","ranks":r,"rep":rep,"ms":ms,"rc":rc,"err":err if rc!=0 else None})
        m = statistics.median([x for x in times if x is not None]) if any(times) else None
        print(f"  mpi-{r}: {m}", flush=True)

(OUT/"lp_mpi_rerun.json").write_text(json.dumps(results,indent=2))
(OUT/"DONE").write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
print(f"DONE -> {OUT}/lp_mpi_rerun.json", flush=True)
