#!/usr/bin/env python3
"""Reconstruct cost_sweep/runs_{standalone,rayon,mpi}.json from raw_runs/cost-*/.

Aggregates are compacted: keep only execution_time_ms / compute_ms / communication_ms
in result.raw (drop distances/labels). Reads canonical raws (never deleted).
"""
import json
import re
import sys
from pathlib import Path

if len(sys.argv) != 2:
    sys.exit(f"Usage: {sys.argv[0]} <campaign_root>")

CAMP = Path(sys.argv[1]).resolve()
OUT = CAMP / "cost_sweep"
OUT.mkdir(parents=True, exist_ok=True)

RAW_DIRS = {
    "standalone": CAMP / "raw_runs" / "cost-standalone",
    "rayon":      CAMP / "raw_runs" / "cost-rayon",
    "mpi":        CAMP / "raw_runs" / "cost-mpi",
}

# cost_<algo>_<backend>_n{n}[_t{t}|_r{r}]_run{i}.json
RE = re.compile(
    r"^cost_(?P<algo>[a-z]+)_(?P<be>[a-z]+)_n(?P<n>\d+)(?:_t(?P<t>\d+)|_r(?P<r>\d+)|_single)?_run(?P<rep>\d+)\.json$"
)

KEEP_RAW = ("execution_time_ms", "compute_ms", "communication_ms", "load_time_ms",
            "build_time_ms", "total_time_ms", "num_edges", "threads", "ranks")


def compact(rec: dict) -> dict:
    res = rec.get("result", {}) or {}
    raw = res.get("raw") or {}
    compact_raw = {k: raw[k] for k in KEEP_RAW if k in raw}
    out = {
        "algorithm": rec.get("algorithm"),
        "backend":   rec.get("backend"),
        "framework": rec.get("framework"),
        "nodes":     rec.get("nodes"),
        "phase":     rec.get("phase"),
        "rep":       rec.get("rep"),
        "status":    res.get("status", "?"),
        "result":    {"raw": compact_raw},
    }
    for k in ("threads", "ranks"):
        if k in rec:
            out[k] = rec[k]
    return out


for backend, d in RAW_DIRS.items():
    rows = []
    if not d.exists():
        print(f"[skip] {d} missing")
        continue
    for f in sorted(d.iterdir()):
        m = RE.match(f.name)
        if not m:
            continue
        try:
            rec = json.loads(f.read_text())
        except Exception as e:
            print(f"[warn] {f.name}: {e}")
            continue
        rows.append(compact(rec))
    out = OUT / f"runs_{backend}.json"
    out.write_text(json.dumps(rows, indent=2))
    n_pass = sum(1 for r in rows if r["status"] == "passed")
    print(f"[ok] {out.name}: {len(rows)} rows ({n_pass} passed)")
