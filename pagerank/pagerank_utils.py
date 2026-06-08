"""PageRank payload generation utilities. Mirror of bfs_utils.py."""
import argparse
import os

AWS_S3_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

DEFAULT_MAX_ITERATIONS = 100
DEFAULT_DAMPING = 0.85
DEFAULT_TOLERANCE = 1e-6


def generate_pagerank_payload(
    endpoint: str,
    partitions: int,
    num_nodes: int,
    bucket: str,
    key: str,
    max_iterations: int | None = None,
    damping: float | None = None,
    tolerance: float | None = None,
    granularity: int = 1,
) -> list[dict]:
    """Build per-worker JSON payloads for the PageRank burst action.

    One dict per partition/worker slot. The Burst controller requires the
    payload count to be divisible by ``granularity``, so coalesced
    executions still need ``partitions`` entries. Each worker slot owns
    one global S3 partition; ``granularity`` is the number of workers
    packed into each Burst invocation.
    """
    payload_list = []
    for i in range(partitions):
        entry: dict = {
            "group_id": i // granularity,
            "partitions": partitions,
            "granularity": granularity,
            "num_nodes": num_nodes,
            "input_data": {
                "bucket": bucket,
                "key": key,
                "endpoint": endpoint,
                "region": AWS_S3_REGION,
                "aws_access_key_id": AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            },
        }
        if max_iterations is not None:
            entry["max_iterations"] = max_iterations
        if damping is not None:
            entry["damping"] = damping
        if tolerance is not None:
            entry["tolerance"] = tolerance
        payload_list.append(entry)
    return payload_list


def add_pagerank_to_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pagerank-endpoint", type=str, required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--num-nodes", type=int, required=True)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--key", type=str, required=True)
    parser.add_argument("--max-iterations", type=int, default=None)
    parser.add_argument("--damping", type=float, default=None)
    parser.add_argument("--tolerance", type=float, default=None)
