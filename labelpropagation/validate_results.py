#!/usr/bin/env python3
import argparse
import json
import os
import sys

import boto3
from botocore.config import Config


UNKNOWN = 2**32 - 1


def make_s3_client(endpoint):
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def normalize_labels(raw, *, source):
    labels = raw.get("labels") if isinstance(raw, dict) else raw
    if isinstance(labels, list):
        return {str(idx): int(label) for idx, label in enumerate(labels)}
    if isinstance(labels, dict):
        return {str(node): int(label) for node, label in labels.items()}
    raise ValueError(f"{source} labels must be either a list or an object")


def load_json_file(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_burst_labels(bucket, key, endpoint):
    output_key = f"{key}/output/labels_final.json"
    s3 = make_s3_client(endpoint)
    response = s3.get_object(Bucket=bucket, Key=output_key)
    return json.loads(response["Body"].read().decode("utf-8"))


def compare_labels(standalone, burst, num_nodes, max_examples=20):
    expected_nodes = {str(idx) for idx in range(num_nodes)}
    missing = sorted(expected_nodes - set(burst.keys()), key=int)
    extra = sorted(set(burst.keys()) - expected_nodes, key=int)
    mismatches = []

    for node in range(num_nodes):
        key = str(node)
        if key in burst and standalone.get(key) != burst[key]:
            mismatches.append((node, standalone.get(key, UNKNOWN), burst[key]))
            if len(mismatches) >= max_examples:
                break

    return missing[:max_examples], extra[:max_examples], mismatches


def main():
    parser = argparse.ArgumentParser(description="Validate Burst LP labels against standalone output")
    parser.add_argument("--standalone", required=True, help="Standalone JSON output path")
    parser.add_argument("--graph", required=False, help="Accepted for benchmark compatibility")
    parser.add_argument("--bucket", required=True, help="S3 bucket containing Burst output")
    parser.add_argument("--key", required=True, help="S3 key prefix used by the Burst run")
    parser.add_argument("--endpoint", required=True, help="S3/MinIO endpoint")
    parser.add_argument("--num-nodes", type=int, required=True, help="Expected number of nodes")
    args = parser.parse_args()

    try:
        standalone = normalize_labels(load_json_file(args.standalone), source="standalone")
        burst = normalize_labels(load_burst_labels(args.bucket, args.key, args.endpoint), source="burst")
    except Exception as exc:
        print(f"Validation setup failed: {exc}", file=sys.stderr)
        return 2

    missing, extra, mismatches = compare_labels(standalone, burst, args.num_nodes)
    if missing or extra or mismatches:
        print("Label validation failed", file=sys.stderr)
        if missing:
            print(f"Missing Burst labels for nodes: {missing}", file=sys.stderr)
        if extra:
            print(f"Unexpected Burst labels for nodes: {extra}", file=sys.stderr)
        if mismatches:
            print("Sample mismatches:", file=sys.stderr)
            for node, expected, actual in mismatches:
                print(f"  node {node}: standalone={expected}, burst={actual}", file=sys.stderr)
        return 1

    print(f"Validation passed: {args.num_nodes} labels match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
