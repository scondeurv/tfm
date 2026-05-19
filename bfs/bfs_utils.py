"""
BFS payload generation utilities.

Mirror of labelpropagation_utils.py for the BFS benchmark.
"""
import argparse
import os

AWS_S3_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

DEFAULT_MAX_LEVELS = 500


def generate_bfs_payload(
    endpoint: str,
    partitions: int,
    num_nodes: int,
    bucket: str,
    key: str,
    source_node: int = 0,
    max_levels: int | None = None,
    granularity: int = 1,
) -> list[dict]:
    """
    Build per-worker JSON payloads for the BFS burst action.

    Returns one dict per partition/worker slot.
    The Burst controller requires the payload count to be divisible by
    `granularity`, so coalesced executions still need `partitions` entries.
    Each worker slot owns one global S3 partition; `granularity` is the number
    of workers packed into each Burst invocation.
    Each dict includes S3 credentials, graph location, and BFS parameters.
    """
    payload_list = []
    num_requests = partitions

    for i in range(num_requests):
        entry: dict = {
            "group_id": i // granularity,
            "partitions": partitions,
            "granularity": granularity,
            "num_nodes": num_nodes,
            "source_node": source_node,
            "input_data": {
                "bucket": bucket,
                "key": key,
                "endpoint": endpoint,
                "region": AWS_S3_REGION,
                "aws_access_key_id": AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            },
        }
        if max_levels is not None:
            entry["max_levels"] = max_levels

        payload_list.append(entry)

    return payload_list


def add_bfs_to_parser(parser: argparse.ArgumentParser) -> None:
    """Add standard BFS CLI arguments to an argparse parser."""
    parser.add_argument(
        "--bfs-endpoint",
        type=str,
        required=True,
        help="S3 endpoint URL where the BFS graph is stored",
    )
    parser.add_argument("--partitions", type=int, required=True, help="Number of S3 partitions")
    parser.add_argument("--num-nodes", type=int, required=True, help="Total node count")
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--key", type=str, required=True, help="S3 key prefix (graph location)")
    parser.add_argument(
        "--source-node", type=int, default=0, help="BFS source node ID (default: 0)"
    )
    parser.add_argument(
        "--max-levels",
        type=int,
        default=None,
        help=f"Maximum BFS depth (default: {DEFAULT_MAX_LEVELS})",
    )
