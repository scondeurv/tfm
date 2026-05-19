import argparse
import os

DEFAULT_CONVERGENCE_THRESHOLD = 0
DEFAULT_OUTPUT = "labelpropagation_payload.json"

AWS_S3_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

def generate_payload(endpoint, partitions, num_nodes, bucket, key, convergence_threshold=DEFAULT_CONVERGENCE_THRESHOLD, max_iterations=None, granularity=1):
    granularity = int(granularity or 1)
    if granularity <= 0:
        raise ValueError("granularity must be positive")
    if partitions % granularity != 0:
        raise ValueError(f"partitions ({partitions}) must be divisible by granularity ({granularity})")

    payload_list = []
    num_requests = partitions
    
    for i in range(num_requests):
        payload_list.append(
            {
                "group_id": i // granularity,
                "partitions": partitions,
                "granularity": granularity,
                "num_nodes": num_nodes,
                "convergence_threshold": int(convergence_threshold),
                **({"max_iterations": max_iterations} if max_iterations is not None else {}),
                "input_data": {
                    "bucket": bucket,
                    "key": key, # Base key, workers will append their id
                    "endpoint": endpoint,
                    "region": AWS_S3_REGION,
                    "aws_access_key_id": AWS_ACCESS_KEY_ID,
                    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                },
            }
        )

    return payload_list

def add_labelpropagation_to_parser(parser):
    parser.add_argument("--lp-endpoint", type=str, required=True,
                        help="Endpoint of the S3 service in which the label propagation graph is stored")
    parser.add_argument("--partitions", type=int, required=True, help="Number of partitions")
    parser.add_argument("--num-nodes", type=int, required=True, help="Number of nodes in the dataset graph")
    parser.add_argument("--bucket", type=str, required=True, help="Label propagation bucket name")
    parser.add_argument("--key", type=str, required=True, help="Label propagation object key")
    parser.add_argument("--max-iterations", type=int, default=None, help="Maximum iterations (default 50)")
    parser.add_argument("--convergence-threshold", type=int, default=DEFAULT_CONVERGENCE_THRESHOLD,
                        help="Stop when total changed labels <= threshold (default 0)")
