"""Auto-generated campaign report (markdown tables + matplotlib figures).

Invoked from `run_cloudlab_campaign.run_report_phase()` after a campaign
finishes (or via `--phase report` standalone to regenerate without re-running
the compute). Idempotent: overwrites prior outputs.

Outputs per algorithm at `<campaign_root>/report/<algo>/`:
  - cost_loglog.png         time vs n, all backends (log-log)
  - cost_speedup.png        speedup vs standalone
  - cost_crossover.json     smallest n where each backend beats standalone
  - cost_table.md           median ms per (backend, n)
  - size_burst_vs_spark.png Burst e2e + compute_only vs Spark
  - cross_backend_table.md  best variant per n across all 5 backends

Outputs at campaign root `<campaign_root>/report/`:
  - summary.md              executive summary + links to all per-algo reports
  - README.md               metadata (timestamp, backends, matrix, hardware)
"""
from __future__ import annotations

import json
import re
import statistics
from pathlib import Path
from typing import Any, Iterable, Optional


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text()) if path.exists() else None


_COST_MPI_RE = re.compile(r"^cost_[a-z]+_mpi_n(\d+)_r(\d+)_run\d+\.json$")
_COST_RAYON_RE = re.compile(r"^cost_[a-z]+_rayon_n(\d+)_t(\d+)_run\d+\.json$")
_COST_STANDALONE_RE = re.compile(r"^cost_[a-z]+_standalone_n(\d+)_single_run\d+\.json$")
_BURST_SIZE_RE = re.compile(r"^[a-z]+_size_sweep_n(\d+)_p(\d+)_g\d+_m\d+_ck\d+_run\d+\.json$")
_SPARK_SIZE_RE = re.compile(r"^size_sweep_[a-z]+_n(\d+)_e(\d+)_m\d+g_run\d+\.json$")


def derive_metadata_from_raw_runs(campaign_root: Path) -> dict[str, Any]:
    """Reconstruct campaign-level fields from raw_runs/ filenames.

    Defensive fallback for missing/empty metadata.json fields. Sources are
    canonical filename patterns; no JSON parse required. Returns sorted lists.
    """
    raw_root = campaign_root / "raw_runs"
    derived: dict[str, set[int]] = {
        "mpi_ranks": set(),
        "rayon_threads": set(),
        "cost_sweep_nodes": set(),
        "size_nodes": set(),
        "burst_partition_list": set(),
        "spark_partition_list": set(),
    }
    backends: set[str] = set()
    if not raw_root.is_dir():
        return {}
    for sub in raw_root.iterdir():
        if not sub.is_dir():
            continue
        name = sub.name
        for f in sub.iterdir():
            fn = f.name
            m = _COST_MPI_RE.match(fn)
            if m:
                backends.add("mpi")
                derived["cost_sweep_nodes"].add(int(m.group(1)))
                derived["mpi_ranks"].add(int(m.group(2)))
                continue
            m = _COST_RAYON_RE.match(fn)
            if m:
                backends.add("rayon")
                derived["cost_sweep_nodes"].add(int(m.group(1)))
                derived["rayon_threads"].add(int(m.group(2)))
                continue
            m = _COST_STANDALONE_RE.match(fn)
            if m:
                backends.add("standalone")
                derived["cost_sweep_nodes"].add(int(m.group(1)))
                continue
            m = _BURST_SIZE_RE.match(fn)
            if m and name == "burst":
                backends.add("burst")
                derived["size_nodes"].add(int(m.group(1)))
                derived["burst_partition_list"].add(int(m.group(2)))
                continue
            m = _SPARK_SIZE_RE.match(fn)
            if m and name == "spark":
                backends.add("spark")
                derived["size_nodes"].add(int(m.group(1)))
                derived["spark_partition_list"].add(int(m.group(2)))
    out: dict[str, Any] = {k: sorted(v) for k, v in derived.items() if v}
    if backends:
        out["backends"] = sorted(backends)
    return out


def _fill_metadata_from_raw(metadata: dict[str, Any], campaign_root: Path) -> dict[str, Any]:
    """Merge derived fields into metadata for any missing/empty entries."""
    derived = derive_metadata_from_raw_runs(campaign_root)
    merged = dict(metadata)
    for key, value in derived.items():
        existing = merged.get(key)
        if not existing:
            merged[key] = value
    return merged


def _samples(rows: list[dict[str, Any]], key: str) -> list[float]:
    out: list[float] = []
    for r in rows:
        raw = (r.get("result") or {}).get("raw") if "result" in r else r.get("raw")
        raw = raw or {}
        v = raw.get(key)
        if isinstance(v, (int, float)) and v > 0:
            out.append(float(v))
    return out


def _summarise_by_n(rows: list[dict[str, Any]], key: str = "execution_time_ms") -> dict[int, dict[str, float]]:
    by_n: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        n = r.get("nodes")
        if n is None:
            continue
        by_n.setdefault(int(n), []).append(r)
    out: dict[int, dict[str, float]] = {}
    for n, grp in by_n.items():
        samples = _samples(grp, key)
        if not samples:
            continue
        mean = statistics.mean(samples)
        cv = (statistics.stdev(samples) / mean * 100.0) if len(samples) >= 2 and mean > 0 else 0.0
        out[n] = {
            "n_samples": len(samples),
            "mean": mean,
            "median": statistics.median(samples),
            "cv": cv,
            "min": min(samples),
            "max": max(samples),
            "p95": sorted(samples)[max(0, int(round(0.95 * len(samples))) - 1)],
        }
    return out


def _variant_key_for(backend: str) -> Optional[str]:
    return {"rayon": "threads", "mpi": "ranks"}.get(backend)


def _split_cost_rows_by_variant(backend: str, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    variant_key = _variant_key_for(backend)
    if variant_key is None:
        return {backend: rows}
    out: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        variant = r.get(variant_key)
        if variant is None:
            raw = (r.get("result") or {}).get("raw") if "result" in r else r.get("raw")
            variant = (raw or {}).get(variant_key)
        label = f"{backend}-{variant}" if variant is not None else backend
        out.setdefault(label, []).append(r)
    return out


def _render_table(summaries: dict[str, dict[int, dict[str, float]]]) -> str:
    labels = sorted(summaries.keys())
    all_n = sorted({n for s in summaries.values() for n in s})
    if not labels or not all_n:
        return "_(no data)_\n"
    header = "| n | " + " | ".join(f"{l} median ms (CV%)" for l in labels) + " |"
    sep = "|---:|" + "|".join("---:" for _ in labels) + "|"
    lines = [header, sep]
    for n in all_n:
        row = [f"{n:,}"]
        for l in labels:
            rec = summaries[l].get(n, {})
            v = rec.get("median")
            if v is None:
                row.append("—")
            else:
                cv = rec.get("cv", 0.0)
                nrep = rec.get("n_samples", 0)
                cv_cell = f"{cv:.1f}%" if nrep >= 2 else "n=1"
                row.append(f"{v:.1f} ({cv_cell})")
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n"


def _find_crossover(
    standalone: dict[int, dict[str, float]],
    other: dict[int, dict[str, float]],
    metric: str = "median",
) -> Optional[int]:
    common = sorted(set(standalone) & set(other))
    for n in common:
        if other[n][metric] < standalone[n][metric]:
            return n
    return None


def render_cost_report(campaign_root: Path, algo_name: str, out_dir: Path) -> None:
    """Generate cost_loglog.png, cost_speedup.png, cost_table.md, cost_crossover.json."""
    cost_dir = campaign_root / "cost_sweep"
    if not cost_dir.exists():
        return

    runs: dict[str, list[dict[str, Any]]] = {}
    for path in cost_dir.glob("runs_*.json"):
        backend = path.stem.removeprefix("runs_")
        rows = _load_json(path) or []
        # filter by algo
        rows = [r for r in rows if r.get("algorithm") in (None, algo_name)]
        if rows:
            runs[backend] = rows

    if not runs:
        return

    summaries: dict[str, dict[int, dict[str, float]]] = {}
    for backend, rows in runs.items():
        for label, sub in _split_cost_rows_by_variant(backend, rows).items():
            summ = _summarise_by_n(sub)
            if summ:
                summaries[label] = summ

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "cost_table.md").write_text(
        f"# COST results — {algo_name.upper()}\n\n"
        + "Median `execution_time_ms` per (backend, n). Variants: rayon-{threads}, mpi-{ranks}.\n\n"
        + _render_table(summaries)
    )

    if "standalone" in summaries:
        crossovers = {
            label: _find_crossover(summaries["standalone"], summ)
            for label, summ in summaries.items()
            if label != "standalone"
        }
        (out_dir / "cost_crossover.json").write_text(json.dumps(crossovers, indent=2))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    fig, ax = plt.subplots(figsize=(8, 6))
    for label, summ in sorted(summaries.items()):
        ns = sorted(summ)
        ys = [summ[n]["median"] for n in ns]
        ax.loglog(ns, ys, "-o", label=label, markersize=4)
    ax.set_xlabel("Graph size (nodes)")
    ax.set_ylabel("execution_time_ms (median)")
    ax.set_title(f"COST {algo_name.upper()}: time vs graph size")
    ax.grid(True, which="both", ls=":", alpha=0.5)
    ax.legend(fontsize=8, loc="best")
    fig.tight_layout()
    fig.savefig(out_dir / "cost_loglog.png", dpi=140)
    plt.close(fig)

    if "standalone" in summaries:
        fig, ax = plt.subplots(figsize=(8, 6))
        std = summaries["standalone"]
        for label, summ in sorted(summaries.items()):
            if label == "standalone":
                continue
            common = sorted(set(std) & set(summ))
            if not common:
                continue
            ys = [std[n]["median"] / summ[n]["median"] for n in common]
            ax.semilogx(common, ys, "-o", label=label, markersize=4)
        ax.axhline(1.0, color="black", ls="--", alpha=0.6, label="standalone")
        ax.set_xlabel("Graph size (nodes)")
        ax.set_ylabel("Speedup over standalone (×)")
        ax.set_title(f"COST {algo_name.upper()}: speedup vs single-thread baseline")
        ax.grid(True, which="both", ls=":", alpha=0.5)
        ax.legend(fontsize=8, loc="best")
        fig.tight_layout()
        fig.savefig(out_dir / "cost_speedup.png", dpi=140)
        plt.close(fig)


def _load_size_sweep_spark(campaign_root: Path, algo_name: Optional[str] = None) -> dict[int, dict[str, float]]:
    """Map n → {e2e_median_ms} aggregated across spark_runs_p*.json.

    Spark cells are typically configured with a single partition count per
    campaign, so we collapse the (n, p) dimension and report one median per
    graph size.
    """
    out: dict[int, list[float]] = {}
    size_dir = campaign_root / "size_sweep"
    if not size_dir.exists():
        return {}
    for pat in ("*_spark_runs_p*.json", "spark_runs_p*.json"):
        for path in size_dir.glob(pat):
            rows = _load_json(path) or []
            for r in rows:
                if r.get("status") not in ("ok", "passed"):
                    continue
                if algo_name is not None and r.get("algorithm") != algo_name:
                    continue
                n = r.get("nodes")
                res = r.get("result") or {}
                # Spark rows store metrics flat under ``result`` (e.g.
                # ``end_to_end_ms``), not nested under ``result.spark`` like
                # burst rows do. Accept both layouts to be safe.
                e2e = res.get("end_to_end_ms")
                if e2e is None and isinstance(res.get("spark"), dict):
                    e2e = res["spark"].get("end_to_end_ms")
                if n is None or not isinstance(e2e, (int, float)):
                    continue
                out.setdefault(int(n), []).append(float(e2e))
    return {
        n: {"e2e_median_ms": statistics.median(vs), "n_e2e": len(vs)}
        for n, vs in out.items()
    }


def _load_size_sweep_burst(campaign_root: Path, algo_name: Optional[str] = None) -> dict[tuple[int, int], dict[str, float]]:
    """Map (nodes, partitions) → {end_to_end_ms_median, compute_only_ms_median}.

    Reads both per-algorithm files (``<algo>_burst_runs_p*.json``) and the
    legacy un-prefixed pattern (``burst_runs_p*.json``). When ``algo_name`` is
    given, only rows whose ``algorithm`` field matches are kept.
    """
    out: dict[tuple[int, int], list[dict[str, Any]]] = {}
    size_dir = campaign_root / "size_sweep"
    if not size_dir.exists():
        return {}
    patterns = ["*_burst_runs_p*.json", "burst_runs_p*.json"]
    seen_paths: set[Path] = set()
    for pat in patterns:
        for path in size_dir.glob(pat):
            if path in seen_paths:
                continue
            seen_paths.add(path)
            try:
                p = int(path.stem.split("_p")[-1])
            except ValueError:
                continue
            rows = _load_json(path) or []
            for r in rows:
                if r.get("status") not in ("ok", "passed"):
                    continue
                if algo_name is not None and r.get("algorithm") != algo_name:
                    continue
                n = r.get("nodes")
                if n is None:
                    continue
                out.setdefault((int(n), p), []).append(r)
    def _warm_clc_ms(burst: dict[str, Any]) -> float | None:
        """Burst "without cold-start" proxy: compute + S3 load + Redis exchange.

        Sums load_ms + compute_ms + communication_ms from phase_metrics. This
        is what the action would take if its container were already alive when
        the activation arrived — i.e., excludes pod spawn / runtime init time
        but includes the per-worker S3 partition download and the reduce +
        broadcast collectives over Redis.
        """
        pm = burst.get("phase_metrics") if isinstance(burst, dict) else None
        if not isinstance(pm, dict):
            return None
        parts = [pm.get(k) for k in ("load_ms", "compute_ms", "communication_ms")]
        nums = [float(v) for v in parts if isinstance(v, (int, float))]
        if not nums:
            return None
        # Require at least compute_ms + (load_ms or communication_ms) to be valid.
        return float(sum(nums))

    summaries: dict[tuple[int, int], dict[str, float]] = {}
    for key, rows in out.items():
        e2e = [
            float(r["result"]["burst"]["end_to_end_ms"])
            for r in rows
            if isinstance((r.get("result") or {}).get("burst", {}).get("end_to_end_ms"), (int, float))
        ]
        co = [
            float(r["result"]["burst"]["compute_only_ms"])
            for r in rows
            if isinstance((r.get("result") or {}).get("burst", {}).get("compute_only_ms"), (int, float))
        ]
        clc = [
            v for r in rows
            if (v := _warm_clc_ms((r.get("result") or {}).get("burst", {}))) is not None
        ]
        summaries[key] = {
            "n_e2e": len(e2e),
            "e2e_median_ms": statistics.median(e2e) if e2e else 0.0,
            "n_co": len(co),
            "co_median_ms": statistics.median(co) if co else 0.0,
            "n_clc": len(clc),
            "clc_median_ms": statistics.median(clc) if clc else 0.0,
        }
    return summaries


def render_size_figures(campaign_root: Path, algo_name: str, out_dir: Path) -> None:
    """Plot Burst e2e + compute_only vs Spark from size_sweep results."""
    size_dir = campaign_root / "size_sweep"
    if not size_dir.exists():
        return
    burst = _load_size_sweep_burst(campaign_root, algo_name)
    if not burst:
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    burst_by_n: dict[int, dict[int, dict[str, float]]] = {}
    for (n, p), v in burst.items():
        burst_by_n.setdefault(n, {})[p] = v

    rows_md = [
        "| n | best p (e2e) | burst e2e cold (ms) | burst warm: compute+S3+Redis (ms) | burst compute_only (ms) |",
        "|---:|---:|---:|---:|---:|",
    ]
    for n in sorted(burst_by_n):
        ps = sorted(burst_by_n[n])
        best_p = min(ps, key=lambda p: burst_by_n[n][p]["e2e_median_ms"])
        rec = burst_by_n[n][best_p]
        clc = rec.get("clc_median_ms", 0.0)
        clc_cell = f"{clc:.1f}" if clc else "—"
        rows_md.append(
            f"| {n:,} | p={best_p} | {rec['e2e_median_ms']:.1f} | {clc_cell} | {rec['co_median_ms']:.1f} |"
        )
    (out_dir / "size_burst_table.md").write_text(
        f"# Burst size_sweep — {algo_name.upper()}\n\n"
        "Columnas:\n"
        "- `burst e2e cold (ms)`: tiempo medido extremo a extremo, incluyendo init de pod, descarga del zip, runtime init, lectura de particiones desde S3, cómputo, escritura a S3 y retorno.\n"
        "- `burst warm: compute+S3+Redis (ms)`: lo mismo **excluyendo cold start** del contenedor. Es `load_ms + compute_ms + communication_ms` agregado: lo que tardaría la celda si los pods ya estuvieran vivos al recibir la activación. Incluye lectura S3 por worker + cómputo + intercambio Redis (reduce + broadcast).\n"
        "- `burst compute_only (ms)`: sólo el cómputo distribuido aislado (span entre la primera iteración y la última), sin contar load/write S3 ni Redis explícito.\n\n"
        + "\n".join(rows_md) + "\n"
    )

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    fig, ax = plt.subplots(figsize=(8, 6))
    for p in sorted({p for (_, p) in burst}):
        xs = sorted(n for (n, pp) in burst if pp == p)
        ys_e2e = [burst[(n, p)]["e2e_median_ms"] for n in xs]
        ax.loglog(xs, ys_e2e, "-o", label=f"burst-p{p} e2e", markersize=4)
    for p in sorted({p for (_, p) in burst}):
        xs = sorted(n for (n, pp) in burst if pp == p)
        ys_co = [burst[(n, p)]["co_median_ms"] for n in xs if burst[(n, p)]["co_median_ms"] > 0]
        xs_co = [n for n in xs if burst[(n, p)]["co_median_ms"] > 0]
        if xs_co:
            ax.loglog(xs_co, ys_co, "--s", label=f"burst-p{p} compute_only", markersize=4, alpha=0.7)
    ax.set_xlabel("Graph size (nodes)")
    ax.set_ylabel("ms (median)")
    ax.set_title(f"Burst {algo_name.upper()}: end-to-end vs compute-only")
    ax.grid(True, which="both", ls=":", alpha=0.5)
    ax.legend(fontsize=8, loc="best")
    fig.tight_layout()
    fig.savefig(out_dir / "size_burst_vs_spark.png", dpi=140)
    plt.close(fig)


def _enumerate_best_per_n(
    cost_summaries: dict[str, dict[int, dict[str, float]]],
    burst_summary: dict[int, dict[int, dict[str, float]]],
) -> list[dict[str, Any]]:
    all_n: set[int] = set()
    for s in cost_summaries.values():
        all_n.update(s)
    all_n.update(burst_summary)
    rows: list[dict[str, Any]] = []
    for n in sorted(all_n):
        entry: dict[str, Any] = {"nodes": n}
        candidates: list[tuple[str, float]] = []
        for label, summ in cost_summaries.items():
            if n in summ:
                candidates.append((label, summ[n]["median"]))
        if n in burst_summary:
            for p, v in burst_summary[n].items():
                if v["e2e_median_ms"] > 0:
                    candidates.append((f"burst-p{p} e2e (cold)", v["e2e_median_ms"]))
                if v.get("clc_median_ms", 0) > 0:
                    candidates.append((f"burst-p{p} warm (compute+S3+Redis)", v["clc_median_ms"]))
                if v["co_median_ms"] > 0:
                    candidates.append((f"burst-p{p} compute_only", v["co_median_ms"]))
        if candidates:
            best = min(candidates, key=lambda t: t[1])
            entry["best_label"] = best[0]
            entry["best_median_ms"] = best[1]
            if "standalone" in cost_summaries and n in cost_summaries["standalone"]:
                entry["standalone_median_ms"] = cost_summaries["standalone"][n]["median"]
                entry["speedup_vs_standalone"] = cost_summaries["standalone"][n]["median"] / best[1]
        rows.append(entry)
    return rows


def render_cross_backend_table(campaign_root: Path, algo_name: str, out_dir: Path) -> None:
    """Cross-backend best-per-n table covering 5 backends."""
    cost_dir = campaign_root / "cost_sweep"
    runs: dict[str, list[dict[str, Any]]] = {}
    if cost_dir.exists():
        for path in cost_dir.glob("runs_*.json"):
            backend = path.stem.removeprefix("runs_")
            rows = [r for r in (_load_json(path) or []) if r.get("algorithm") in (None, algo_name)]
            if rows:
                runs[backend] = rows
    cost_summaries: dict[str, dict[int, dict[str, float]]] = {}
    for backend, rows in runs.items():
        for label, sub in _split_cost_rows_by_variant(backend, rows).items():
            summ = _summarise_by_n(sub)
            if summ:
                cost_summaries[label] = summ
    burst = _load_size_sweep_burst(campaign_root, algo_name)
    burst_by_n: dict[int, dict[int, dict[str, float]]] = {}
    for (n, p), v in burst.items():
        burst_by_n.setdefault(n, {})[p] = v
    spark_by_n = _load_size_sweep_spark(campaign_root, algo_name)

    # Feed Spark into the best-per-n picker by adding it as a synthetic
    # cost-style summary keyed under "spark". _enumerate_best_per_n will then
    # consider Spark as another candidate; if it never wins (typical) it
    # still gets surfaced in the per-row supplementary columns below.
    cost_summaries_extended = dict(cost_summaries)
    if spark_by_n:
        cost_summaries_extended["spark"] = {
            n: {"median": v["e2e_median_ms"], "n": v.get("n_e2e", 0)}
            for n, v in spark_by_n.items()
        }

    rows = _enumerate_best_per_n(cost_summaries_extended, burst_by_n)
    if not rows:
        return

    lines = [
        "| n | best backend | best median (ms) | standalone (ms) | speedup × | burst e2e cold (ms) | burst warm (compute+S3+Redis) (ms) | spark (ms) |",
        "|---:|:---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        n = r["nodes"]
        # Best Burst cold across all measured partitions (lowest e2e_median).
        burst_e2e_cell = "—"
        burst_warm_cell = "—"
        if n in burst_by_n:
            e2e_vals = [v["e2e_median_ms"] for v in burst_by_n[n].values() if v.get("e2e_median_ms", 0) > 0]
            clc_vals = [v.get("clc_median_ms", 0.0) for v in burst_by_n[n].values() if v.get("clc_median_ms", 0) > 0]
            if e2e_vals:
                burst_e2e_cell = f"{min(e2e_vals):.0f}"
            if clc_vals:
                burst_warm_cell = f"{min(clc_vals):.0f}"
        spark_cell = "—"
        if n in spark_by_n:
            spark_cell = f"{spark_by_n[n]['e2e_median_ms']:.0f}"
        lines.append(
            "| {n} | {best} | {best_ms:.1f} | {std} | {sp} | {bcold} | {bwarm} | {spark} |".format(
                n=f"{n:,}",
                best=r.get("best_label", "—"),
                best_ms=r.get("best_median_ms", 0.0) or 0.0,
                std=f"{r['standalone_median_ms']:.1f}" if "standalone_median_ms" in r else "—",
                sp=f"{r['speedup_vs_standalone']:.2f}" if "speedup_vs_standalone" in r else "—",
                bcold=burst_e2e_cell,
                bwarm=burst_warm_cell,
                spark=spark_cell,
            )
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "cross_backend_table.md").write_text(
        f"# Cross-backend best per n — {algo_name.upper()}\n\n"
        + "Median time at each graph size across los 5 backends. La columna "
        + "**best backend** se queda con el ganador (el más rápido en mediana). "
        + "Las columnas auxiliares muestran Burst e2e (cold), Burst sin cold "
        + "start (cómputo + S3 read + Redis) y Spark e2e — los tres tipos de "
        + "número que no aparecen como ganadores pero conviene comparar.\n\n"
        + "\n".join(lines)
        + "\n"
    )


def render_warmpool_breakdown(campaign_root: Path, algo_name: str, out_dir: Path) -> None:
    """Emit `warmpool_breakdown.md` per algo: cold vs warm median per cell.

    Reads raw_runs/burst/<algo>_size_sweep_n<N>_p<P>_*.json files and tabulates
    the warm-pool block written by run_burst() under --burst-warmup-shots>0.
    Cells without a `warmpool` block (legacy single-shot) are skipped silently.
    """
    raw_dir = campaign_root / "raw_runs" / "burst"
    if not raw_dir.exists():
        return
    rows: list[dict[str, Any]] = []
    pattern = f"{algo_name}_size_sweep_n*.json"
    for path in sorted(raw_dir.glob(pattern)):
        data = _load_json(path)
        if not isinstance(data, dict):
            continue
        wp = data.get("warmpool")
        if not isinstance(wp, dict):
            continue
        cold = wp.get("cold_e2e_ms")
        warm_med = wp.get("warm_e2e_median_ms")
        sub_med = wp.get("warm_subtotal_median_ms")
        ratio = (cold / warm_med) if (cold and warm_med) else None
        warm_over_sub = (warm_med / sub_med) if (warm_med and sub_med) else None
        rows.append({
            "n": data.get("nodes"),
            "p": data.get("partitions"),
            "cold_e2e_ms": cold,
            "warm_e2e_median_ms": warm_med,
            "warm_subtotal_median_ms": sub_med,
            "cold_over_warm": ratio,
            "warm_over_subtotal": warm_over_sub,
            "warm_shots": wp.get("shots"),
        })
    if not rows:
        return
    rows.sort(key=lambda r: (r["n"] or 0, r["p"] or 0))
    lines = [
        "# Warm-pool breakdown — " + algo_name,
        "",
        "Each row aggregates one Burst cell measured under the warm-pool protocol:",
        "1 cold invocation (discarded) + N warm invocations reusing the same OW pool.",
        "`warm_e2e_median_ms` is the canonical cell metric (replaces legacy cold-only e2e).",
        "",
        "| n | p | warm shots | cold e2e (ms) | warm median e2e (ms) | warm median subtotal (ms) | cold/warm | warm/subtotal |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            "| {n} | {p} | {ws} | {cold} | {warm} | {sub} | {ratio} | {wos} |".format(
                n=r["n"],
                p=r["p"],
                ws=r.get("warm_shots") if r.get("warm_shots") is not None else "-",
                cold=f"{r['cold_e2e_ms']:.0f}" if r["cold_e2e_ms"] is not None else "-",
                warm=f"{r['warm_e2e_median_ms']:.0f}" if r["warm_e2e_median_ms"] is not None else "-",
                sub=f"{r['warm_subtotal_median_ms']:.0f}" if r["warm_subtotal_median_ms"] is not None else "-",
                ratio=f"{r['cold_over_warm']:.2f}" if r["cold_over_warm"] is not None else "-",
                wos=f"{r['warm_over_subtotal']:.2f}" if r["warm_over_subtotal"] is not None else "-",
            )
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "warmpool_breakdown.md").write_text("\n".join(lines) + "\n")


def render_campaign_summary(campaign_root: Path, algo_name: str) -> None:
    """Append-or-create campaign-level summary.md + README.md."""
    report_root = campaign_root / "report"
    report_root.mkdir(parents=True, exist_ok=True)

    readme = report_root / "README.md"
    metadata_path = campaign_root / "metadata.json"
    metadata = _load_json(metadata_path) or {}
    metadata = _fill_metadata_from_raw(metadata, campaign_root)
    readme_lines = [
        f"# Campaign report — {campaign_root.name}",
        "",
        f"- Created: `{metadata.get('created_at', 'unknown')}`",
        f"- Backends: `{','.join(metadata.get('backends', []))}`",
        f"- Cost sweep nodes: `{metadata.get('cost_sweep_nodes', [])}`",
        f"- Size sweep nodes: `{metadata.get('size_nodes', [])}`",
        f"- Burst partitions: `{metadata.get('burst_partition_list', [])}`",
        f"- Rayon threads: `{metadata.get('rayon_threads', [])}`",
        f"- MPI ranks: `{metadata.get('mpi_ranks', [])} on hosts {metadata.get('mpi_hosts', 'compute6,compute7')}`",
        f"- MPI map-by: `{metadata.get('mpi_map_by', 'default')}`",
        f"- MPI TCP interface include: `{metadata.get('mpi_btl_if_include', 'default')}`",
        "",
        "## Per-algorithm reports",
        "",
    ]
    for sub in sorted(p for p in report_root.iterdir() if p.is_dir()):
        readme_lines.append(f"- [{sub.name}]({sub.name}/)")
    readme.write_text("\n".join(readme_lines) + "\n")

    summary_md = report_root / "summary.md"
    summary_lines = [
        f"# Executive summary — {campaign_root.name}",
        "",
        f"Algorithms processed: {sorted(p.name for p in report_root.iterdir() if p.is_dir())}",
        "",
        "## Files of interest",
        "",
    ]
    for algo_dir in sorted(p for p in report_root.iterdir() if p.is_dir()):
        for artefact in (
            "cost_table.md",
            "cross_backend_table.md",
            "size_burst_table.md",
            "cost_loglog.png",
            "cost_speedup.png",
            "size_burst_vs_spark.png",
        ):
            target = algo_dir / artefact
            if target.exists():
                summary_lines.append(f"- [{algo_dir.name}/{artefact}]({algo_dir.name}/{artefact})")
    summary_md.write_text("\n".join(summary_lines) + "\n")


__all__ = [
    "render_cost_report",
    "render_size_figures",
    "render_cross_backend_table",
    "render_campaign_summary",
    "render_warmpool_breakdown",
    "derive_metadata_from_raw_runs",
]
