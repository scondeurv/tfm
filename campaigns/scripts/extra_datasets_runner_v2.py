#!/usr/bin/env python3
"""V-cost + NUMA on 3 extra real graphs. Runs ENTIRELY on compute6.

v2 fixes (2026-06-15):
  - n = max_id + 1 autodetect (SNAP graphs are 1-indexed with gaps).
  - source = high-out-degree vertex (vertex 0 may not exist).
  - MPI single-host only (compute6:64) — cross-host 1GbE saturates on big graphs.
  - Per-cell timeout 600s.
  - Incremental JSONL write (append per cell) — no loss on crash.
  - Log stderr tail on failure.

Datasets (SNAP):
  com-orkut    : ~3M nodes, 117M edges (social, dense)
  web-Google   : ~916k nodes, 5.1M edges (web, sparse)
  roadNet-CA   : ~1.97M nodes, 5.5M edges (road, high-diameter)

Output: /home/users/sconde/extra_datasets/results_<ts>/{dataset}/{vcost,numa}.jsonl
Log:    /tmp/extra_datasets.log
"""
from __future__ import annotations
import gzip
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

SRC = Path("/home/users/sconde/src")
DATA = Path("/home/users/sconde/datasets")
DATA.mkdir(parents=True, exist_ok=True)
TS = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
OUT = Path(f"/home/users/sconde/extra_datasets/results_{TS}")
OUT.mkdir(parents=True, exist_ok=True)
MPI_PREFIX = "/home/users/sconde/opt/openmpi-4.1.5"

DATASETS = [
    {
        "name": "com-orkut",
        "url": "https://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz",
        "comment": "#",
    },
    {
        "name": "web-Google",
        "url": "https://snap.stanford.edu/data/web-Google.txt.gz",
        "comment": "#",
    },
    {
        "name": "roadNet-CA",
        "url": "https://snap.stanford.edu/data/roadNet-CA.txt.gz",
        "comment": "#",
    },
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def download_and_prepare(ds: dict) -> tuple[Path, Path]:
    plain = DATA / f"{ds['name']}.tsv"
    weighted = DATA / f"{ds['name']}-weighted.tsv"
    if plain.exists() and weighted.exists():
        log(f"{ds['name']}: already prepared")
        return plain, weighted
    raw_gz = DATA / f"{ds['name']}.txt.gz"
    if not raw_gz.exists():
        log(f"{ds['name']}: downloading {ds['url']}")
        urllib.request.urlretrieve(ds["url"], str(raw_gz))
    log(f"{ds['name']}: extracting + cleaning")
    cmt = ds["comment"]
    with gzip.open(raw_gz, "rt") as fin, open(plain, "w") as fp, open(weighted, "w") as fw:
        import random
        random.seed(42)
        for line in fin:
            s = line.rstrip("\r\n")
            if not s or s.startswith(cmt):
                continue
            parts = s.split("\t") if "\t" in s else s.split()
            if len(parts) < 2:
                continue
            try:
                u = int(parts[0]); v = int(parts[1])
            except ValueError:
                continue
            fp.write(f"{u}\t{v}\n")
            fw.write(f"{u}\t{v}\t{random.randint(1,100)}\n")
    log(f"{ds['name']}: wrote {plain.name} + {weighted.name}")
    return plain, weighted


def scan_graph(plain: Path) -> tuple[int, int]:
    """Return (n = max_id+1, source = vertex with highest out-degree)."""
    max_id = 0
    out_deg: dict[int, int] = {}
    with open(plain) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 2:
                continue
            u = int(parts[0]); v = int(parts[1])
            if u > max_id:
                max_id = u
            if v > max_id:
                max_id = v
            out_deg[u] = out_deg.get(u, 0) + 1
    source = max(out_deg, key=out_deg.get)
    return max_id + 1, source


# ─────────── algo command builders ───────────

def algo_meta(algo: str):
    if algo == "lp":
        return ("labelpropagation",
                "lpst/target/release/label-propagation",
                "lp-rayon/target/release/lp-rayon",
                "lp-mpi/target/release/lp-mpi")
    if algo == "bfs":
        return ("bfs",
                "bfs-standalone/target/release/bfs-standalone",
                "bfs-rayon/target/release/bfs-rayon",
                "bfs-mpi/target/release/bfs-mpi")
    if algo == "sssp":
        return ("sssp",
                "sssp-standalone/target/release/sssp-standalone",
                "sssp-rayon/target/release/sssp-rayon",
                "sssp-mpi/target/release/sssp-mpi")
    if algo == "pagerank":
        return ("pagerank",
                "pagerank-standalone/target/release/pagerank-standalone",
                "pagerank-rayon/target/release/pagerank-rayon",
                "pagerank-mpi/target/release/pagerank-mpi")
    raise ValueError(algo)


MAX_ITER = 20
MAX_LEVELS = 10000  # large for high-diameter road network


def tail_for(algo: str, source: int) -> list[str]:
    if algo == "bfs":
        return [str(source), str(MAX_LEVELS)]
    if algo == "sssp":
        return [str(source), str(MAX_ITER)]
    return [str(MAX_ITER)]


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


def run(cmd: str, env: dict | None = None, timeout: int = 600) -> tuple[int, str, str, float]:
    t0 = time.time()
    try:
        p = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True,
                           env={**os.environ, **(env or {})}, timeout=timeout)
        return p.returncode, p.stdout, p.stderr, time.time() - t0
    except subprocess.TimeoutExpired as e:
        return -1, e.stdout or "", (e.stderr or "") + f"\nTIMEOUT after {timeout}s", time.time() - t0


def vcost_cell(algo: str, n: int, source: int, graph: Path, backend: str, **kw) -> dict:
    d, sa_bin, ray_bin, mpi_bin = algo_meta(algo)
    wd = SRC / d
    tail = " ".join(tail_for(algo, source))
    if backend == "standalone":
        cmd = f"cd {wd} && {wd}/{sa_bin} {graph} {n} {tail}"
        rc, so, se, w = run(cmd, timeout=900)
        rec = {"backend": backend}
    elif backend == "rayon":
        t = kw["threads"]
        cmd = f"cd {wd} && RAYON_NUM_THREADS={t} {wd}/{ray_bin} {graph} {n} {tail} {t}"
        rc, so, se, w = run(cmd, timeout=900)
        rec = {"backend": backend, "threads": t}
    elif backend == "mpi":
        r = kw["ranks"]
        hosts = kw.get("hosts", "compute6:64")  # SINGLE-HOST: no cross-host
        mpirun = f"{MPI_PREFIX}/bin/mpirun -np {r} --prefix {MPI_PREFIX}"
        mca = "--mca btl tcp,self --mca btl_tcp_if_include 192.168.5.0/24 --mca oob_tcp_if_include 192.168.5.0/24"
        cmd = f"cd {wd} && {mpirun} {mca} -H {hosts} {wd}/{mpi_bin} {graph} {n} {tail}"
        rc, so, se, w = run(cmd, timeout=900)
        rec = {"backend": backend, "ranks": r}
    else:
        raise ValueError(backend)
    parsed = parse_json_last(so)
    rec.update({
        "rc": rc,
        "wall_s": round(w, 1),
        "status": "passed" if (rc == 0 and parsed and parsed.get("execution_time_ms") is not None) else "failed",
        "execution_time_ms": parsed.get("execution_time_ms") if parsed else None,
        "reachable": (parsed.get("reachable_nodes") if parsed else None),
        "error": (se[-300:] if rc != 0 else None),
    })
    return rec


def numa_cell(algo: str, n: int, source: int, graph: Path, config: str) -> dict:
    d, _, ray_bin, mpi_bin = algo_meta(algo)
    wd = SRC / d
    tail = " ".join(tail_for(algo, source))
    if config == "naive-64":
        cmd = f"cd {wd} && RAYON_NUM_THREADS=64 {wd}/{ray_bin} {graph} {n} {tail} 64"
    elif config == "local-32":
        cmd = f"cd {wd} && RAYON_NUM_THREADS=32 numactl --cpunodebind=0 --membind=0 {wd}/{ray_bin} {graph} {n} {tail} 32"
    elif config == "interleave-64":
        cmd = f"cd {wd} && RAYON_NUM_THREADS=64 numactl --interleave=all {wd}/{ray_bin} {graph} {n} {tail} 64"
    elif config == "mpi-per-socket":
        mpirun = f"{MPI_PREFIX}/bin/mpirun -np 2 --prefix {MPI_PREFIX} --map-by numa --bind-to numa"
        cmd = f"cd {wd} && {mpirun} {wd}/{mpi_bin} {graph} {n} {tail}"
    else:
        raise ValueError(config)
    rc, so, se, w = run(cmd, timeout=900)
    parsed = parse_json_last(so)
    return {
        "config": config,
        "rc": rc,
        "wall_s": round(w, 1),
        "status": "passed" if (rc == 0 and parsed and parsed.get("execution_time_ms") is not None) else "failed",
        "execution_time_ms": parsed.get("execution_time_ms") if parsed else None,
        "reachable": (parsed.get("reachable_nodes") if parsed else None),
        "error": (se[-300:] if rc != 0 else None),
    }


# ─────────── main loop ───────────
RAYON_THREADS = [1, 8, 32, 64]
MPI_RANKS = [2, 4, 8, 16]
NUMA_CONFIGS = ["naive-64", "local-32", "interleave-64", "mpi-per-socket"]
REPS = 3


def append_jsonl(path: Path, rec: dict) -> None:
    with open(path, "a") as f:
        f.write(json.dumps(rec) + "\n")


def run_vcost(ds: dict, plain: Path, weighted: Path, n: int, source: int, out_path: Path) -> None:
    base = {"dataset": ds["name"], "n": n, "source": source}
    for algo in ["lp", "bfs", "sssp", "pagerank"]:
        graph = weighted if algo == "sssp" else plain
        for rep in range(REPS):
            log(f"VCOST {ds['name']}/{algo}/standalone rep{rep}")
            rec = vcost_cell(algo, n, source, graph, "standalone")
            rec.update({**base, "algo": algo, "rep": rep})
            append_jsonl(out_path, rec)
            log(f"  {rec['status']} ms={rec['execution_time_ms']} wall={rec['wall_s']}s")
            if rec["status"] == "failed" and rec.get("error"):
                log(f"  STDERR: {rec['error'][:200]}")
        for t in RAYON_THREADS:
            for rep in range(REPS):
                log(f"VCOST {ds['name']}/{algo}/rayon-{t} rep{rep}")
                rec = vcost_cell(algo, n, source, graph, "rayon", threads=t)
                rec.update({**base, "algo": algo, "rep": rep})
                append_jsonl(out_path, rec)
                log(f"  {rec['status']} ms={rec['execution_time_ms']} wall={rec['wall_s']}s")
                if rec["status"] == "failed" and rec.get("error"):
                    log(f"  STDERR: {rec['error'][:200]}")
        for r in MPI_RANKS:
            for rep in range(REPS):
                log(f"VCOST {ds['name']}/{algo}/mpi-{r} rep{rep}")
                rec = vcost_cell(algo, n, source, graph, "mpi", ranks=r)
                rec.update({**base, "algo": algo, "rep": rep})
                append_jsonl(out_path, rec)
                log(f"  {rec['status']} ms={rec['execution_time_ms']} wall={rec['wall_s']}s")
                if rec["status"] == "failed" and rec.get("error"):
                    log(f"  STDERR: {rec['error'][:200]}")


def run_numa(ds: dict, plain: Path, weighted: Path, n: int, source: int, out_path: Path) -> None:
    base = {"dataset": ds["name"], "n": n, "source": source}
    for algo in ["lp", "bfs", "sssp", "pagerank"]:
        graph = weighted if algo == "sssp" else plain
        for cfg in NUMA_CONFIGS:
            for rep in range(REPS):
                log(f"NUMA {ds['name']}/{algo}/{cfg} rep{rep}")
                rec = numa_cell(algo, n, source, graph, cfg)
                rec.update({**base, "algo": algo, "rep": rep})
                append_jsonl(out_path, rec)
                log(f"  {rec['status']} ms={rec['execution_time_ms']} wall={rec['wall_s']}s")
                if rec["status"] == "failed" and rec.get("error"):
                    log(f"  STDERR: {rec['error'][:200]}")


def main():
    log(f"START v2. Output -> {OUT}")
    for ds in DATASETS:
        ds_dir = OUT / ds["name"]
        ds_dir.mkdir(parents=True, exist_ok=True)
        try:
            plain, weighted = download_and_prepare(ds)
        except Exception as e:
            log(f"FAILED prepare {ds['name']}: {e}")
            continue
        log(f"{ds['name']}: scanning graph for n + source")
        n, source = scan_graph(plain)
        log(f"{ds['name']}: n={n} source={source}")
        (ds_dir / "meta.json").write_text(json.dumps({"n": n, "source": source}))
        log(f"==== {ds['name']} VCOST ====")
        run_vcost(ds, plain, weighted, n, source, ds_dir / "vcost.jsonl")
        log(f"==== {ds['name']} NUMA ====")
        run_numa(ds, plain, weighted, n, source, ds_dir / "numa.jsonl")
        log(f"==== {ds['name']} DONE ====")
    log(f"ALL DONE. Output: {OUT}")
    (OUT / "DONE").write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))


if __name__ == "__main__":
    main()
