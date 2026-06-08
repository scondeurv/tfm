#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"
RESULT_PREFIX = "SPARK_BENCHMARK_RESULT_JSON:"

@dataclass(frozen=True)
class SparkAlgorithm:
    slug: str
    x_key: str
    points: list[int]
    generator_repo: str | None
    generator_script: str | None
    dataset_name_fn: Any
    generator_args_fn: Any
    submit_script: str | None
    submit_args_fn: Any
    configuration: dict[str, Any]
    supported: bool = True
    unsupported_reason: str | None = None


def checkpoint_path(algorithm: SparkAlgorithm) -> Path:
    return RESULTS_DIR / f"{algorithm.slug}.partial.json"


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def repo_python(repo_name: str) -> Path:
    candidates = [
        ROOT / repo_name / ".venv/bin/python",
        ROOT / "labelpropagation/.venv/bin/python",
        ROOT / "bfs/.venv/bin/python",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            probe = subprocess.run(
                [str(candidate), "--version"],
                text=True,
                capture_output=True,
                timeout=10,
            )
            if probe.returncode == 0:
                return candidate
        except Exception:
            continue
    return Path(sys.executable)


def mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return float(values[0]), 0.0
    return float(statistics.mean(values)), float(statistics.pstdev(values))


def build_spark_phase_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    load_ms = float(payload.get("load_time_ms", 0.0))
    compute_ms = float(payload.get("compute_only_ms", payload["execution_time_ms"]))
    write_ms = float(payload.get("output_write_ms", 0.0))
    end_to_end_ms = float(payload.get("end_to_end_ms", payload["total_time_ms"]))
    warm_total_ms = load_ms + compute_ms + write_ms
    cold_start_ms = max(0.0, end_to_end_ms - warm_total_ms)
    return {
        "workers": None,
        "cold_start_ms": cold_start_ms,
        "stagger_ms": 0.0,
        "load_ms": load_ms,
        "compute_ms": compute_ms,
        "reduce_ms": 0.0,
        "broadcast_ms": 0.0,
        "communication_ms": 0.0,
        "warm_total_ms": warm_total_ms,
        "host_total_ms": end_to_end_ms,
        "span_ms": compute_ms,
        "write_ms": write_ms,
        "iterations": None,
        "per_iteration": [],
    }


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(RESULTS_DIR, 0o777)


def remove_output_dir(host_output_dir: Path, container_output_dir: str) -> None:
    if not host_output_dir.exists():
        return
    try:
        shutil.rmtree(host_output_dir)
        return
    except PermissionError:
        pass

    cleanup = subprocess.run(
        ["docker", "exec", "spark-master", "rm", "-rf", container_output_dir],
        text=True,
        capture_output=True,
    )
    if cleanup.returncode != 0:
        raise RuntimeError(
            f"failed to clean Spark output directory {container_output_dir}\n"
            f"STDOUT:\n{cleanup.stdout}\nSTDERR:\n{cleanup.stderr}"
        )
    if host_output_dir.exists():
        shutil.rmtree(host_output_dir)


def parse_result(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        if line.startswith(RESULT_PREFIX):
            return json.loads(line[len(RESULT_PREFIX):])
    raise ValueError("spark benchmark output did not contain a structured JSON result")


def run_command(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "command failed with exit code "
            f"{completed.returncode}: {' '.join(command)}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )
    return completed


def ensure_dataset(algorithm: SparkAlgorithm, size: int, force: bool) -> Path | None:
    if not algorithm.generator_repo or not algorithm.generator_script:
        return None

    dataset_path = DATA_DIR / algorithm.dataset_name_fn(size)
    if dataset_path.exists() and not force:
        return dataset_path

    python = repo_python(algorithm.generator_repo)
    script = ROOT / algorithm.generator_repo / algorithm.generator_script
    command = [str(python), str(script), *algorithm.generator_args_fn(size, dataset_path)]
    log(f"Generating dataset for {algorithm.slug} at {dataset_path.name}")
    run_command(command, cwd=script.parent)
    if not dataset_path.exists():
        raise RuntimeError(f"dataset generation did not produce {dataset_path}")
    return dataset_path


def benchmark_point(
    algorithm: SparkAlgorithm,
    size: int,
    runs: int,
) -> dict[str, Any]:
    dataset_path = DATA_DIR / algorithm.dataset_name_fn(size)
    container_input = f"/opt/tfm-spark/data/{dataset_path.name}"
    load_runs: list[float] = []
    compute_runs: list[float] = []
    exec_runs: list[float] = []
    write_runs: list[float] = []
    total_runs: list[float] = []
    last_payload: dict[str, Any] | None = None
    run_records: list[dict[str, Any]] = []

    for run_idx in range(runs):
        output_path = f"/opt/tfm-spark/results/{algorithm.slug}_{size}_run{run_idx + 1}"
        host_output_dir = RESULTS_DIR / f"{algorithm.slug}_{size}_run{run_idx + 1}"
        remove_output_dir(host_output_dir, output_path)
        command = ["bash", str(HERE / "scripts" / algorithm.submit_script), *algorithm.submit_args_fn(container_input, output_path, size)]
        log(f"Running Spark {algorithm.slug} size={size:,} run={run_idx + 1}/{runs}")
        env = os.environ.copy()
        for key, value in algorithm.configuration.get("spark_env", {}).items():
            env[key] = str(value)
        env["SPARK_PERSIST_OUTPUT"] = "true"
        completed = subprocess.run(
            command,
            cwd=HERE,
            text=True,
            capture_output=True,
            env=env,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "command failed with exit code "
                f"{completed.returncode}: {' '.join(command)}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
            )
        payload = parse_result(completed.stdout)
        phase_metrics = build_spark_phase_metrics(payload)
        load_runs.append(float(payload["load_time_ms"]))
        compute_runs.append(float(payload.get("compute_only_ms", payload["execution_time_ms"])))
        exec_runs.append(float(payload["execution_time_ms"]))
        write_runs.append(float(payload.get("output_write_ms", 0.0)))
        total_runs.append(float(payload.get("end_to_end_ms", payload["total_time_ms"])))
        last_payload = payload

        run_records.append(
            {
                "run_index": run_idx + 1,
                "load_time_ms": float(payload["load_time_ms"]),
                "compute_only_ms": float(payload.get("compute_only_ms", payload["execution_time_ms"])),
                "output_write_ms": float(payload.get("output_write_ms", 0.0)),
                "execution_time_ms": float(payload["execution_time_ms"]),
                "end_to_end_ms": float(payload.get("end_to_end_ms", payload["total_time_ms"])),
                "total_time_ms": float(payload["total_time_ms"]),
                "phase_metrics": phase_metrics,
            }
        )

    load_mean, load_std = mean_std(load_runs)
    compute_mean, compute_std = mean_std(compute_runs)
    exec_mean, exec_std = mean_std(exec_runs)
    write_mean, write_std = mean_std(write_runs)
    total_mean, total_std = mean_std(total_runs)
    row = {
        algorithm.x_key: size,
        "spark_load_ms": round(load_mean, 2),
        "spark_load_std_ms": round(load_std, 2),
        "spark_compute_only_ms": round(compute_mean, 2),
        "spark_compute_only_std_ms": round(compute_std, 2),
        "spark_exec_ms": round(exec_mean, 2),
        "spark_exec_std_ms": round(exec_std, 2),
        "spark_output_write_ms": round(write_mean, 2),
        "spark_output_write_std_ms": round(write_std, 2),
        "spark_end_to_end_ms": round(total_mean, 2),
        "spark_end_to_end_std_ms": round(total_std, 2),
        "spark_total_ms": round(total_mean, 2),
        "spark_total_std_ms": round(total_std, 2),
        "spark_load_runs_ms": [round(value, 2) for value in load_runs],
        "spark_compute_only_runs_ms": [round(value, 2) for value in compute_runs],
        "spark_exec_runs_ms": [round(value, 2) for value in exec_runs],
        "spark_output_write_runs_ms": [round(value, 2) for value in write_runs],
        "spark_end_to_end_runs_ms": [round(value, 2) for value in total_runs],
        "spark_total_runs_ms": [round(value, 2) for value in total_runs],
        "phase_metrics": build_spark_phase_metrics(last_payload) if last_payload else None,
    }
    if last_payload:
        for key in ("visited_nodes", "max_level", "reachable_nodes", "max_distance", "iterations", "converged", "labeled_nodes", "distinct_labels", "num_components", "component_hash", "modularity", "num_communities", "num_passes"):
            if key in last_payload:
                row[key] = last_payload[key]
    row["run_records"] = run_records
    return row


def result_path(algorithm: SparkAlgorithm) -> Path:
    return RESULTS_DIR / f"{algorithm.slug}.json"


def write_unsupported_result(algorithm: SparkAlgorithm) -> Path:
    payload = {
        "algorithm": algorithm.slug,
        "framework": "spark",
        "supported": False,
        "reason": algorithm.unsupported_reason,
        "timestamp": datetime.now().isoformat(),
    }
    out_path = result_path(algorithm)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def run_algorithm(algorithm: SparkAlgorithm, runs: int, force: bool, force_data: bool) -> Path:
    out_path = result_path(algorithm)
    partial_path = checkpoint_path(algorithm)
    if out_path.exists() and not force:
        log(f"Skipping Spark {algorithm.slug}: result already present at {out_path}")
        return out_path

    if not algorithm.supported:
        log(f"Recording unsupported Spark baseline for {algorithm.slug}")
        return write_unsupported_result(algorithm)

    rows = []
    completed_points: set[int] = set()
    if partial_path.exists() and not force:
        partial_payload = json.loads(partial_path.read_text(encoding="utf-8"))
        rows = partial_payload.get("results", [])
        completed_points = {int(row[algorithm.x_key]) for row in rows if algorithm.x_key in row}
        if completed_points:
            log(f"Resuming Spark {algorithm.slug} from checkpoint: {sorted(completed_points)}")

    for size in algorithm.points:
        if size in completed_points:
            log(f"Skipping Spark {algorithm.slug} size={size:,} (checkpointed)")
            continue
        ensure_dataset(algorithm, size, force=force_data)
        rows.append(benchmark_point(algorithm, size, runs))
        partial_payload = {
            "algorithm": algorithm.slug,
            "framework": "spark",
            "supported": True,
            "timestamp": datetime.now().isoformat(),
            "runs_per_point": runs,
            "test_points": algorithm.points,
            "results": rows,
            "configuration": algorithm.configuration,
            "partial": True,
        }
        if "experimental_note" in algorithm.configuration:
            partial_payload["experimental_note"] = algorithm.configuration["experimental_note"]
        partial_path.write_text(json.dumps(partial_payload, indent=2), encoding="utf-8")

    payload = {
        "algorithm": algorithm.slug,
        "framework": "spark",
        "supported": True,
        "timestamp": datetime.now().isoformat(),
        "runs_per_point": runs,
        "test_points": algorithm.points,
        "results": rows,
        "configuration": algorithm.configuration,
    }
    if "experimental_note" in algorithm.configuration:
        payload["experimental_note"] = algorithm.configuration["experimental_note"]
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if partial_path.exists():
        partial_path.unlink()
    log(f"Wrote Spark results for {algorithm.slug}: {out_path}")
    return out_path


def build_algorithms() -> dict[str, SparkAlgorithm]:
    return {
        "bfs": SparkAlgorithm(
            slug="bfs",
            x_key="nodes",
            points=[100_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000],
            generator_repo="bfs",
            generator_script="setup_large_bfs_data.py",
            dataset_name_fn=lambda size: f"large_bfs_{size}.txt",
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", "4",
                "--density", "10",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_script="submit-bfs.sh",
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "0",
                "500",
                "4",
            ],
            configuration={
                "partitions": 4,
                "source_node": 0,
                "max_levels": 500,
                "density": 10,
                "spark_env": {
                    "SPARK_TOTAL_EXECUTOR_CORES": "4",
                    "SPARK_EXECUTOR_CORES": "1",
                    "SPARK_EXECUTOR_MEMORY": "4g",
                    "SPARK_DRIVER_MEMORY": "8g",
                    "SPARK_DEFAULT_PARALLELISM": "4",
                    "SPARK_SHUFFLE_PARTITIONS": "4",
                },
            },
        ),
        "sssp": SparkAlgorithm(
            slug="sssp",
            x_key="nodes",
            points=[100_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000],
            generator_repo="sssp",
            generator_script="setup_large_sssp_data.py",
            dataset_name_fn=lambda size: f"large_sssp_{size}.txt",
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", "4",
                "--density", "10",
                "--max-weight", "10.0",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_script="submit-sssp.sh",
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "0",
                "4",
                "500",
            ],
            configuration={
                "partitions": 4,
                "source_node": 0,
                "max_iter": 500,
                "density": 10,
                "max_weight": 10.0,
                "spark_env": {
                    "SPARK_TOTAL_EXECUTOR_CORES": "4",
                    "SPARK_EXECUTOR_CORES": "1",
                    "SPARK_EXECUTOR_MEMORY": "4g",
                    "SPARK_DEFAULT_PARALLELISM": "4",
                    "SPARK_SHUFFLE_PARTITIONS": "4",
                },
            },
        ),
        "pagerank": SparkAlgorithm(
            slug="pagerank",
            x_key="nodes",
            points=[100_000, 500_000, 1_000_000, 2_000_000, 5_000_000],
            generator_repo="pagerank",
            generator_script="setup_large_pagerank_data.py",
            dataset_name_fn=lambda size: f"large_pagerank_{size}.txt",
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", "4",
                "--density", "10",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_script="submit-pagerank.sh",
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "4",
                "100",
                "0.000001",
                "0.85",
            ],
            configuration={
                "partitions": 4,
                "max_iter": 100,
                "damping": 0.85,
                "tolerance": 1e-6,
                "density": 10,
                "spark_env": {
                    "SPARK_TOTAL_EXECUTOR_CORES": "4",
                    "SPARK_EXECUTOR_CORES": "1",
                    "SPARK_EXECUTOR_MEMORY": "4g",
                    "SPARK_DEFAULT_PARALLELISM": "4",
                    "SPARK_SHUFFLE_PARTITIONS": "4",
                },
            },
        ),
        "labelpropagation": SparkAlgorithm(
            slug="labelpropagation",
            x_key="nodes",
            points=[100_000, 500_000, 1_000_000, 2_000_000],
            generator_repo="labelpropagation",
            generator_script="setup_large_lp_data.py",
            dataset_name_fn=lambda size: f"large_lp_{size}.txt",
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", "4",
                "--density", "10",
                "--model", "random",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_script="submit-lp.sh",
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "50",
                "4",
            ],
            configuration={
                "partitions": 4,
                "max_iter": 50,
                "density": 10,
                "experimental_note": (
                    "La baseline Spark para Label Propagation se truncó en 2M nodos "
                    "porque a partir de 3M el coste por repetición superó el presupuesto "
                    "experimental razonable de la campaña."
                ),
                "spark_env": {
                    "SPARK_TOTAL_EXECUTOR_CORES": "4",
                    "SPARK_EXECUTOR_CORES": "1",
                    "SPARK_EXECUTOR_MEMORY": "4g",
                    "SPARK_DEFAULT_PARALLELISM": "4",
                    "SPARK_SHUFFLE_PARTITIONS": "4",
                },
            },
        ),
    }


def parse_points_arg(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(token.strip()) for token in raw.split(",") if token.strip()]


def with_resource_overrides(
    algorithm: SparkAlgorithm,
    *,
    partitions: int,
    executors: int,
    executor_memory: str,
    points: list[int] | None,
) -> SparkAlgorithm:
    if not algorithm.supported:
        return algorithm

    config = json.loads(json.dumps(algorithm.configuration))
    config["partitions"] = partitions
    spark_env = config.setdefault("spark_env", {})
    spark_env["SPARK_TOTAL_EXECUTOR_CORES"] = str(executors)
    spark_env["SPARK_EXECUTOR_CORES"] = "1"
    spark_env["SPARK_EXECUTOR_MEMORY"] = executor_memory
    spark_env.setdefault("SPARK_DRIVER_MEMORY", executor_memory)
    spark_env["SPARK_DEFAULT_PARALLELISM"] = str(partitions)
    spark_env["SPARK_SHUFFLE_PARTITIONS"] = str(partitions)

    if algorithm.slug == "bfs":
        return replace(
            algorithm,
            points=points or algorithm.points,
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", str(partitions),
                "--density", "10",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "0",
                "500",
                str(partitions),
            ],
            configuration=config,
        )

    if algorithm.slug == "sssp":
        return replace(
            algorithm,
            points=points or algorithm.points,
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", str(partitions),
                "--density", "10",
                "--max-weight", "10.0",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "0",
                str(partitions),
                "500",
            ],
            configuration=config,
        )

    if algorithm.slug == "labelpropagation":
        return replace(
            algorithm,
            points=points or algorithm.points,
            generator_args_fn=lambda size, dataset: [
                "--nodes", str(size),
                "--partitions", str(partitions),
                "--density", "10",
                "--model", "random",
                "--output", str(dataset),
                "--no-s3",
            ],
            submit_args_fn=lambda container_input, output_path, _size: [
                container_input,
                output_path,
                "50",
                str(partitions),
            ],
            configuration=config,
        )

    return replace(algorithm, points=points or algorithm.points, configuration=config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark baselines for the graph algorithms used in the TFM.")
    parser.add_argument(
        "--algorithms",
        default="bfs,sssp,labelpropagation",
        help="Comma-separated Spark baseline steps to run.",
    )
    parser.add_argument("--runs", type=int, default=3, help="Repetitions per point.")
    parser.add_argument("--force", action="store_true", help="Re-run even if the JSON already exists.")
    parser.add_argument("--force-data", action="store_true", help="Re-generate local datasets even if they exist.")
    parser.add_argument("--partitions", type=int, default=4, help="Dataset partitions and Spark parallelism.")
    parser.add_argument("--executors", type=int, default=4, help="Spark executors with 1 core each.")
    parser.add_argument("--executor-memory", default="4g", help="Memory per Spark executor.")
    parser.add_argument("--points", default="", help="Comma-separated override for graph sizes.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    algorithms = build_algorithms()
    override_points = parse_points_arg(args.points)
    requested = [token.strip() for token in args.algorithms.split(",") if token.strip()]
    deduped: list[str] = []
    for name in requested:
        if name not in deduped:
            deduped.append(name)
    requested = deduped
    unknown = [name for name in requested if name not in algorithms]
    if unknown:
        raise SystemExit(f"Unknown Spark algorithms: {', '.join(unknown)}")

    for name in requested:
        algorithm = with_resource_overrides(
            algorithms[name],
            partitions=args.partitions,
            executors=args.executors,
            executor_memory=args.executor_memory,
            points=override_points,
        )
        run_algorithm(algorithm, runs=args.runs, force=args.force, force_data=args.force_data)


if __name__ == "__main__":
    main()
