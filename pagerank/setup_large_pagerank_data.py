#!/usr/bin/env python3
"""Generate synthetic directed graphs for PageRank benchmarking.

Same topology family as LP/BFS/SSSP generators: every node i gets `density`
random outgoing edges to distinct nodes (no self-loops), with a fixed RNG seed
for determinism. Output is a TSV `<src>\t<dst>` per line.

Cohabits with the campaign orchestrator's expectation of a
`setup_large_<algo>_data.py` driver that accepts the same flag set as the
LP/BFS/SSSP variants.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np


def generate_pagerank_graph(
    num_nodes: int,
    num_partitions: int,
    output_local: Path,
    *,
    density: int = 10,
    seed: int = 42,
) -> None:
    print(f"Generating PageRank random graph: {num_nodes:,} nodes, density={density}, seed={seed}...")
    rng = np.random.default_rng(seed)
    total_edges = num_nodes * density
    src = np.repeat(np.arange(num_nodes, dtype=np.int64), density)
    raw = rng.integers(0, num_nodes - 1, size=total_edges, dtype=np.int64)
    dst = np.where(raw >= src, raw + 1, raw)

    print(f"  Writing local file: {output_local}")
    output_local.parent.mkdir(parents=True, exist_ok=True)
    with open(output_local, "w") as f:
        for s, d in zip(src.tolist(), dst.tolist()):
            f.write(f"{s}\t{d}\n")
    print(f"  Wrote {total_edges:,} edges → {output_local}")


def _upload_partitions(
    edges_file: Path,
    bucket: str,
    prefix: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    num_partitions: int,
) -> None:
    """Mirror LP/BFS/SSSP partition-and-upload layout: split edges by
    `src % num_partitions` and upload each shard to MinIO."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print("boto3 not installed — skipping S3 upload (--no-s3 implied).")
        return
    print(f"Uploading {num_partitions} partitions to s3://{bucket}/{prefix}")
    shards: dict[int, list[str]] = {p: [] for p in range(num_partitions)}
    with open(edges_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if not parts:
                continue
            try:
                shard = int(parts[0]) % num_partitions
            except ValueError:
                continue
            shards[shard].append(line)
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )
    for shard, rows in shards.items():
        key = f"{prefix}/partition-{shard:04d}.tsv"
        body = "\n".join(rows) + "\n"
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
        print(f"  uploaded {key}  ({len(rows):,} edges)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate large PageRank graph datasets")
    parser.add_argument("--nodes", type=int, required=True)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--bucket", type=str, default="test-bucket")
    parser.add_argument("--prefix", type=str, default=None)
    parser.add_argument("--endpoint", type=str, default="localhost:9000")
    parser.add_argument("--access-key", type=str, default="minioadmin")
    parser.add_argument("--secret-key", type=str, default="minioadmin")
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 upload")
    parser.add_argument("--density", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    output = Path(args.output or f"large_pagerank_{args.nodes}.txt")
    generate_pagerank_graph(
        args.nodes, args.partitions, output,
        density=args.density, seed=args.seed,
    )

    if not args.no_s3 and args.prefix:
        _upload_partitions(
            output, args.bucket, args.prefix, args.endpoint,
            args.access_key, args.secret_key, args.partitions,
        )


if __name__ == "__main__":
    main()
