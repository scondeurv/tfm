#!/usr/bin/env python3
"""
Generate large random graph datasets for BFS benchmarking.

Creates a local .txt graph file and, optionally, S3 partitions for the burst
worker.  Each node gets `density` random outgoing edges, producing
a low-diameter (~log N) graph that is ideal for demonstrating the BFS crossover.

Uses numpy for O(N × density) generation (vs the naive O(N²) approach).
The same file format as LP graphs (TSV: src\\tdst) is used, allowing the same
S3 loading code in ow-bfs to reuse ow-lp's partition reader without changes.
"""
import argparse
import os
import numpy as np


def generate_bfs_graph(
    num_nodes: int,
    num_partitions: int,
    output_local: str,
    bucket: str | None = None,
    s3_prefix: str | None = None,
    endpoint: str = "localhost:9000",
    access_key: str | None = None,
    secret_key: str | None = None,
    density: int = 10,
    seed: int = 42,
):
    """
    Generate a random directed graph for BFS benchmarking.

    Every node i gets `density` random outgoing edges to distinct nodes
    (no self-loops).  Uses numpy for O(N × density) generation.
    Uses a fixed seed so the same parameters always produce the same graph.

    Args:
        density: Outgoing edges per node  (typical: 10–20)
        seed:    RNG seed for reproducibility
    """
    print(f"Generating BFS graph: {num_nodes:,} nodes, density={density}, seed={seed}...")

    rng = np.random.default_rng(seed)
    total_edges = num_nodes * density

    # src[k] = source node of edge k
    src = np.repeat(np.arange(num_nodes, dtype=np.int64), density)

    # Sample targets from [0, num_nodes-1], then apply offset to avoid self-loops:
    # for each edge k, if raw[k] >= src[k], shift by +1  →  target in [0,N] \ {src}
    raw = rng.integers(0, num_nodes - 1, size=total_edges, dtype=np.int64)
    dst = np.where(raw >= src, raw + 1, raw)

    # Build edge strings efficiently
    edges = [f"{s}\t{d}" for s, d in zip(src.tolist(), dst.tolist())]

    print(f"  Generated {len(edges):,} edges total")

    # ── Write local file ────────────────────────────────────────────────────
    print(f"  Writing local file: {output_local}")
    with open(output_local, "w") as f:
        f.write("\n".join(edges))

    # ── Upload to S3 ────────────────────────────────────────────────────────
    if bucket and s3_prefix:
        import boto3
        from botocore.client import Config

        access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
        ep = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
        print(f"  Uploading to S3: {bucket}/{s3_prefix}/ ({num_partitions} partitions)")
        s3 = boto3.client(
            "s3",
            endpoint_url=ep,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

        try:
            s3.create_bucket(Bucket=bucket)
        except Exception:
            pass  # already exists

        # Partition by src % num_partitions
        partitions: list[list[str]] = [[] for _ in range(num_partitions)]
        for edge in edges:
            src_node = int(edge.split("\t")[0])
            partitions[src_node % num_partitions].append(edge)

        for i, part_edges in enumerate(partitions):
            if part_edges:
                data = "\n".join(part_edges).encode("utf-8")
                key = f"{s3_prefix}/part-{i:05d}"
                s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")
                print(f"    ✅ Partition {i}: {len(part_edges):,} edges")

    print(f"✅ BFS graph ready: {num_nodes:,} nodes, {len(edges):,} edges")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate large BFS graph datasets (local file + S3 partitions)"
    )
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--partitions", type=int, default=8, help="Number of S3 partitions")
    parser.add_argument("--output", type=str, default=None, help="Local output file path")
    parser.add_argument("--bucket", type=str, default="test-bucket", help="S3 bucket name")
    parser.add_argument("--prefix", type=str, default=None, help="S3 key prefix")
    parser.add_argument("--endpoint", type=str, default="localhost:9000", help="S3 endpoint URL")
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 upload (local file only)")
    parser.add_argument(
        "--density", type=int, default=10, help="Outgoing edges per node (default: 10)"
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    parser.add_argument("--source", type=int, default=0, help="BFS source node (informational)")

    args = parser.parse_args()

    if args.output is None:
        args.output = f"large_bfs_{args.nodes}.txt"
    if args.prefix is None:
        args.prefix = f"graphs/large-bfs-{args.nodes}"

    generate_bfs_graph(
        num_nodes=args.nodes,
        num_partitions=args.partitions,
        output_local=args.output,
        bucket=None if args.no_s3 else args.bucket,
        s3_prefix=None if args.no_s3 else args.prefix,
        endpoint=args.endpoint,
        density=args.density,
        seed=args.seed,
    )
