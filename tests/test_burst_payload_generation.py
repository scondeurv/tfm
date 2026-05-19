import unittest

from bfs.bfs_utils import generate_bfs_payload
from labelpropagation.labelpropagation_utils import generate_payload as generate_lp_payload
from sssp.sssp_utils import generate_sssp_payload


class BurstPayloadGenerationTests(unittest.TestCase):
    def assert_payload_shape(self, payloads, partitions, granularity):
        self.assertEqual(len(payloads), partitions)
        self.assertEqual(len(payloads) % granularity, 0)
        self.assertEqual(payloads[0]["group_id"], 0)
        self.assertEqual(payloads[-1]["group_id"], (partitions - 1) // granularity)

        for index, payload in enumerate(payloads):
            self.assertEqual(payload["partitions"], partitions)
            self.assertEqual(payload["granularity"], granularity)
            self.assertEqual(payload["group_id"], index // granularity)

    def test_graph_payloads_keep_one_entry_per_partition(self):
        cases = [
            generate_bfs_payload("http://minio", 8, 100, "bucket", "graphs", granularity=4),
            generate_sssp_payload("http://minio", 8, 100, "bucket", "graphs", granularity=8),
            generate_lp_payload("http://minio", 8, 100, "bucket", "graphs", granularity=4),
        ]

        for payloads in cases:
            with self.subTest(payload_count=len(payloads)):
                self.assert_payload_shape(payloads, partitions=8, granularity=payloads[0]["granularity"])


if __name__ == "__main__":
    unittest.main()
