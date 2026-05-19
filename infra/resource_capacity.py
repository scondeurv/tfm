from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import os
import re


_MEMINFO = Path("/proc/meminfo")


@dataclass(frozen=True)
class HostCapacity:
    logical_cpus: int
    total_memory_mb: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class HostBudget:
    host: HostCapacity
    reserved_cpus: int
    reserved_memory_mb: int

    @property
    def usable_cpus(self) -> int:
        return max(1, self.host.logical_cpus - self.reserved_cpus)

    @property
    def usable_memory_mb(self) -> int:
        return max(1024, self.host.total_memory_mb - self.reserved_memory_mb)

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["usable_cpus"] = self.usable_cpus
        payload["usable_memory_mb"] = self.usable_memory_mb
        return payload


@dataclass(frozen=True)
class ResourceRequest:
    framework: str
    cpus: float
    memory_mb: int
    details: dict[str, int | float | str]

    def fits(self, budget: HostBudget) -> bool:
        return self.cpus <= budget.usable_cpus and self.memory_mb <= budget.usable_memory_mb

    def to_dict(self, budget: HostBudget | None = None) -> dict:
        payload = asdict(self)
        if budget is not None:
            payload["fits_budget"] = self.fits(budget)
        return payload


def detect_host_capacity() -> HostCapacity:
    logical_cpus = os.cpu_count() or 1
    total_memory_mb = _read_total_memory_mb()
    return HostCapacity(logical_cpus=logical_cpus, total_memory_mb=total_memory_mb)


def _read_total_memory_mb() -> int:
    if _MEMINFO.exists():
        for line in _MEMINFO.read_text(encoding="utf-8").splitlines():
            if not line.startswith("MemTotal:"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            kib = int(parts[1])
            return max(1, kib // 1024)
    raise RuntimeError("could not determine total memory from /proc/meminfo")


def parse_memory_to_mb(raw: str | int) -> int:
    if isinstance(raw, int):
        return raw
    token = raw.strip().lower()
    match = re.fullmatch(r"(\d+)([a-z]+)?", token)
    if not match:
        raise ValueError(f"invalid memory value: {raw}")
    value = int(match.group(1))
    unit = match.group(2) or "m"
    if unit in {"m", "mb", "mi", "mib"}:
        return value
    if unit in {"g", "gb", "gi", "gib"}:
        return value * 1024
    raise ValueError(f"unsupported memory unit: {raw}")


def divisors(value: int) -> list[int]:
    return [candidate for candidate in range(1, value + 1) if value % candidate == 0]


def burst_cluster_request(
    *,
    workers: int,
    memory_per_worker_mb: int,
    cpu_per_worker: int = 1,
    system_reserved_cpus: int = 6,
    system_reserved_mem_mb: int = 8192,
) -> ResourceRequest:
    return ResourceRequest(
        framework="burst",
        cpus=float(workers * cpu_per_worker + system_reserved_cpus),
        memory_mb=workers * memory_per_worker_mb + system_reserved_mem_mb,
        details={
            "workers": workers,
            "cpu_per_worker": cpu_per_worker,
            "memory_per_worker_mb": memory_per_worker_mb,
            "system_reserved_cpus": system_reserved_cpus,
            "system_reserved_mem_mb": system_reserved_mem_mb,
        },
    )


def spark_cluster_request(
    *,
    executors: int,
    executor_cores: int = 1,
    executor_memory: str | int = "4g",
    master_cpus: float = 1.0,
    master_memory: str | int = "1g",
) -> ResourceRequest:
    executor_memory_mb = parse_memory_to_mb(executor_memory)
    master_memory_mb = parse_memory_to_mb(master_memory)
    return ResourceRequest(
        framework="spark",
        cpus=float(executors * executor_cores) + float(master_cpus),
        memory_mb=executors * executor_memory_mb + master_memory_mb,
        details={
            "executors": executors,
            "executor_cores": executor_cores,
            "executor_memory_mb": executor_memory_mb,
            "master_cpus": master_cpus,
            "master_memory_mb": master_memory_mb,
        },
    )


def feasible_request_rows(
    rows: list[dict],
    *,
    request_builder,
    budget: HostBudget,
) -> list[dict]:
    materialized: list[dict] = []
    for row in rows:
        request = request_builder(row)
        payload = dict(row)
        payload["request"] = request.to_dict(budget)
        payload["feasible"] = request.fits(budget)
        materialized.append(payload)
    return materialized


def max_request(rows: list[dict]) -> dict | None:
    feasible = [row for row in rows if row.get("feasible")]
    if not feasible:
        return None
    cpus = max(float(row["request"]["cpus"]) for row in feasible)
    memory_mb = max(int(row["request"]["memory_mb"]) for row in feasible)
    return {
        "cpus": cpus,
        "memory_mb": memory_mb,
    }
