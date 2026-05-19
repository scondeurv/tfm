from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
INFRA = ROOT / "infra"
if str(INFRA) not in sys.path:
    sys.path.insert(0, str(INFRA))

from resource_capacity import (
    HostBudget,
    HostCapacity,
    burst_cluster_request,
    divisors,
    parse_memory_to_mb,
    spark_cluster_request,
)


def normalize_granularities(partitions: int) -> list[int]:
    return [value for value in divisors(partitions) if value <= 16]


def burst_worker_count(partitions: int, granularity: int) -> int:
    return partitions // granularity


def spark_executor_candidates(partitions: int) -> list[int]:
    return [partitions // 2]


class ResourceCapacityTests(unittest.TestCase):
    def test_parse_memory_to_mb(self) -> None:
        self.assertEqual(parse_memory_to_mb("4g"), 4096)
        self.assertEqual(parse_memory_to_mb("3072m"), 3072)
        self.assertEqual(parse_memory_to_mb(2048), 2048)

    def test_divisors(self) -> None:
        self.assertEqual(divisors(12), [1, 2, 3, 4, 6, 12])

    def test_burst_worker_count_uses_logical_partitions_and_granularity(self) -> None:
        self.assertEqual(burst_worker_count(32, 2), 16)
        self.assertEqual(burst_worker_count(32, 4), 8)
        self.assertEqual(normalize_granularities(32), [1, 2, 4, 8, 16])

    def test_spark_partition_mapping_keeps_p32_feasible(self) -> None:
        self.assertEqual(spark_executor_candidates(32), [16])

    def test_burst_request_matches_planned_budget(self) -> None:
        request = burst_cluster_request(
            workers=16,
            memory_per_worker_mb=2048,
            system_reserved_cpus=6,
            system_reserved_mem_mb=8192,
        )
        self.assertEqual(request.cpus, 22.0)
        self.assertEqual(request.memory_mb, 40960)

    def test_spark_request_matches_planned_budget(self) -> None:
        request = spark_cluster_request(
            executors=16,
            executor_memory="3g",
            master_cpus=1.0,
            master_memory="1g",
        )
        self.assertEqual(request.cpus, 17.0)
        self.assertEqual(request.memory_mb, 50176)

    def test_budget_fit(self) -> None:
        budget = HostBudget(
            host=HostCapacity(logical_cpus=32, total_memory_mb=63933),
            reserved_cpus=2,
            reserved_memory_mb=8192,
        )
        self.assertTrue(
            burst_cluster_request(
                workers=16,
                memory_per_worker_mb=2048,
                system_reserved_cpus=6,
                system_reserved_mem_mb=8192,
            ).fits(budget)
        )
        self.assertFalse(
            burst_cluster_request(
                workers=32,
                memory_per_worker_mb=4096,
                system_reserved_cpus=6,
                system_reserved_mem_mb=8192,
            ).fits(budget)
        )


if __name__ == "__main__":
    unittest.main()
