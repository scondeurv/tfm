#!/usr/bin/env python3
"""
Generate large graph datasets for Label Propagation benchmarking.
Creates a local .txt graph file and, optionally, S3 partitions for Burst.

Supports two graph models:
  --model random  (default) Random directed graph, same structure as BFS/SSSP.
                  density=10 outgoing edges per node, numpy RNG seed=42.
                  Unsupervised: 2-column edges (src\tdst), no seed labels.
  --model ring    Legacy ring graph where node i connects to i+1..i+density.
                  Seeds: 10% of nodes, 4 label groups by position in ring.
"""
import argparse
import os
import numpy as np


def generate_random_graph(num_nodes, num_partitions, output_local, bucket=None, s3_prefix=None,
                          endpoint="localhost:9000", access_key="minioadmin", secret_key="minioadmin",
                          density=10, seed=42):
    """
    Generate a random directed graph for LP benchmarking.

    Same topology as BFS/SSSP: every node i gets `density` random outgoing edges
    to distinct nodes (no self-loops), generated with numpy RNG seed.
    Unsupervised: edges are 2-column (src\tdst) with no seed labels, so the
    solver initializes every node to its own id and converges by majority vote.
    """
    print(f"Generating LP random graph: {num_nodes:,} nodes, density={density}, seed={seed}...")

    rng = np.random.default_rng(seed)
    total_edges = num_nodes * density

    src = np.repeat(np.arange(num_nodes, dtype=np.int64), density)
    raw = rng.integers(0, num_nodes - 1, size=total_edges, dtype=np.int64)
    dst = np.where(raw >= src, raw + 1, raw)

    # Unsupervised LP: emit 2 columns only (src\tdst, no seed labels).
    # Empty initial_labels -> every node starts as its own label (labels[i]=i),
    # canonical Raghavan-2007 majority-vote community detection.
    edges = [f"{s}\t{d}" for s, d in zip(src.tolist(), dst.tolist())]

    print(f"  Generated {len(edges):,} edges total")
    _write_and_upload(edges, num_partitions, output_local, bucket, s3_prefix, endpoint, access_key, secret_key)


def generate_ring_graph(num_nodes, num_partitions, output_local, bucket=None, s3_prefix=None,
                        endpoint="localhost:9000", access_key="minioadmin", secret_key="minioadmin",
                        density=10):
    """
    Legacy ring graph: node i connects to (i+1)%N, (i+2)%N, ..., (i+density)%N.
    Seeds: 10% of nodes, 4 label groups based on position in ring.
    """
    print(f"Generating LP ring graph: {num_nodes:,} nodes, density={density}...")

    edges = []
    for i in range(num_nodes):
        for offset in range(1, density + 1):
            dst = (i + offset) % num_nodes
            if i % 10 == 0 and offset == 1:
                group_size = max(1, num_nodes // 4)
                label = (i // group_size) * 100
                edges.append(f"{i}\t{dst}\t{label}")
            else:
                edges.append(f"{i}\t{dst}")

    print(f"  Generated {len(edges):,} edges total")
    _write_and_upload(edges, num_partitions, output_local, bucket, s3_prefix, endpoint, access_key, secret_key)


def _write_and_upload(edges, num_partitions, output_local, bucket, s3_prefix, endpoint, access_key, secret_key):
    print(f"  Writing local file: {output_local}")
    with open(output_local, 'w') as f:
        f.write('\n'.join(edges))

    if bucket and s3_prefix:
        import boto3
        from botocore.client import Config

        ep = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
        print(f"  Uploading to S3: {bucket}/{s3_prefix}/ ({num_partitions} partitions)")
        s3 = boto3.client(
            's3',
            endpoint_url=ep,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1',
        )
        try:
            s3.create_bucket(Bucket=bucket)
        except Exception:
            pass

        partitions = [[] for _ in range(num_partitions)]
        for edge in edges:
            src_node = int(edge.split('\t')[0])
            partitions[src_node % num_partitions].append(edge)

        for i, part_edges in enumerate(partitions):
            data = '\n'.join(part_edges).encode('utf-8')
            s3.put_object(Bucket=bucket, Key=f"{s3_prefix}/part-{i:05d}",
                          Body=data, ContentType="text/plain")
            print(f"    Partition {i}: {len(part_edges):,} edges")

    print(f"Graph generation complete: {len(edges):,} edges")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate large LP graph datasets")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--partitions", type=int, default=8, help="Number of S3 partitions")
    parser.add_argument("--output", type=str, default=None, help="Local output file")
    parser.add_argument("--bucket", type=str, default="test-bucket", help="S3 bucket")
    parser.add_argument("--prefix", type=str, default=None, help="S3 key prefix")
    parser.add_argument("--endpoint", type=str, default="localhost:9000", help="S3 endpoint")
    parser.add_argument(
        "--aws-access-key-id",
        default=None,
        help="S3 access key id (defaults to AWS_ACCESS_KEY_ID or minioadmin)",
    )
    parser.add_argument(
        "--aws-secret-access-key",
        default=None,
        help="S3 secret access key (defaults to AWS_SECRET_ACCESS_KEY or minioadmin)",
    )
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 upload")
    parser.add_argument("--density", type=int, default=10, help="Edges per node (default: 10)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (random model only)")
    parser.add_argument("--model", choices=["random", "ring"], default="random",
                        help="Graph model: 'random' (default, comparable to BFS/SSSP) or 'ring' (legacy)")

    args = parser.parse_args()

    if args.output is None:
        args.output = f"large_lp_{args.nodes}.txt"
    if args.prefix is None:
        args.prefix = f"graphs/large-{args.nodes}"

    access_key = args.aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = args.aws_secret_access_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    kwargs = dict(
        num_nodes=args.nodes,
        num_partitions=args.partitions,
        output_local=args.output,
        bucket=None if args.no_s3 else args.bucket,
        s3_prefix=None if args.no_s3 else args.prefix,
        endpoint=args.endpoint,
        access_key=access_key,
        secret_key=secret_key,
        density=args.density,
    )

    if args.model == "random":
        generate_random_graph(**kwargs, seed=args.seed)
    else:
        generate_ring_graph(**kwargs)
