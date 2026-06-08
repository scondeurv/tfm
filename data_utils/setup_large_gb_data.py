#!/usr/bin/env python3
"""
Download and prepare Higgs dataset for Gradient Boosting benchmarking.

Creates both a local binary file for the standalone binary and S3 partitions
for the burst worker.  The Higgs dataset (11M rows, 28 numeric features) is
a standard benchmark for gradient boosting algorithms.

Data format (binary, little-endian):
  Header:  [num_rows: u32] [num_features: u16]
  Labels:  [label_0: u8] [label_1: u8] ... [label_{N-1}: u8]
  Features (column-major, f32):
    feature_0: [row_0: f32] ... [row_{N-1}: f32]
    feature_1: [row_0: f32] ... [row_{N-1}: f32]
    ...
    feature_27: [row_0: f32] ... [row_{N-1}: f32]

Bin edges for histogram-based split finding are stored as a separate S3 object.
"""
import argparse
import io
import os
import struct
import gzip
import urllib.request

import numpy as np
import boto3
from botocore.client import Config

HIGGS_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz"
HIGGS_CACHE = "higgs_full.csv.gz"
NUM_FEATURES = 28
NUM_BINS = 256


def _cache_looks_valid(cache_path: str) -> bool:
    """Cheap integrity check for the cached gzip before reusing it."""
    try:
        if not os.path.exists(cache_path) or os.path.getsize(cache_path) == 0:
            return False
        with gzip.open(cache_path, "rt") as handle:
            first_line = handle.readline().strip()
        if not first_line:
            return False
        return len(first_line.split(",")) >= 29
    except Exception:
        return False


def download_higgs(cache_path: str = HIGGS_CACHE) -> str:
    """Download Higgs dataset if not already cached."""
    if os.path.exists(cache_path):
        if _cache_looks_valid(cache_path):
            print(f"  Using cached Higgs dataset: {cache_path}")
            return cache_path
        print(f"  Cached Higgs dataset looks invalid, removing: {cache_path}")
        os.remove(cache_path)

    print(f"  Downloading Higgs dataset from UCI...")
    print(f"  URL: {HIGGS_URL}")
    print(f"  This may take several minutes (~2.6 GB compressed)...")

    urllib.request.urlretrieve(HIGGS_URL, cache_path)
    if not _cache_looks_valid(cache_path):
        raise RuntimeError(f"Downloaded Higgs dataset is invalid or incomplete: {cache_path}")
    print(f"  ✅ Downloaded to {cache_path}")
    return cache_path


def load_higgs(cache_path: str, max_rows: int | None = None) -> tuple[np.ndarray, np.ndarray]:
    """
    Load Higgs CSV into numpy arrays.

    Returns (features, labels) where:
      features: shape (N, 28), dtype float32
      labels: shape (N,), dtype uint8  (0 or 1)
    """
    print(f"  Loading Higgs data (max_rows={max_rows})...")

    rows_features = []
    rows_labels = []
    count = 0

    with gzip.open(cache_path, "rt") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 29:
                continue
            label = int(float(parts[0]))
            feats = [float(x) for x in parts[1:29]]
            rows_labels.append(label)
            rows_features.append(feats)
            count += 1
            if max_rows and count >= max_rows:
                break
            if count % 1_000_000 == 0:
                print(f"    Loaded {count:,} rows...")

    features = np.array(rows_features, dtype=np.float32)
    labels = np.array(rows_labels, dtype=np.uint8)
    print(f"  ✅ Loaded {len(labels):,} rows × {NUM_FEATURES} features")
    return features, labels


def create_subset(
    features: np.ndarray,
    labels: np.ndarray,
    num_rows: int,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Shuffle and take first num_rows (deterministic with seed)."""
    rng = np.random.default_rng(seed)
    idx = rng.permutation(len(labels))[:num_rows]
    return features[idx], labels[idx]


def compute_bin_edges(features: np.ndarray, num_bins: int = NUM_BINS) -> np.ndarray:
    """
    Compute quantile-based bin edges for histogram binning.

    Returns shape (num_features, num_bins - 1) with the split thresholds.
    """
    num_features = features.shape[1]
    edges = np.zeros((num_features, num_bins - 1), dtype=np.float32)
    quantiles = np.linspace(0, 100, num_bins + 1)[1:-1]  # 255 quantile points

    for f in range(num_features):
        col = features[:, f]
        pcts = np.percentile(col, quantiles)
        # Deduplicate: keep unique, pad with inf
        unique = np.unique(pcts)
        if len(unique) < num_bins - 1:
            padded = np.full(num_bins - 1, np.inf, dtype=np.float32)
            padded[: len(unique)] = unique
            edges[f] = padded
        else:
            edges[f] = unique[: num_bins - 1]

    return edges


def digitize_features(features: np.ndarray, bin_edges: np.ndarray) -> np.ndarray:
    """
    Convert continuous features to bin indices (0..255).

    Returns shape (N, num_features), dtype uint8.
    """
    num_rows, num_features = features.shape
    binned = np.zeros((num_rows, num_features), dtype=np.uint8)

    for f in range(num_features):
        valid_edges = bin_edges[f][bin_edges[f] < np.inf]
        if len(valid_edges) > 0:
            binned[:, f] = np.digitize(features[:, f], valid_edges).astype(np.uint8)
        else:
            binned[:, f] = 0

    return binned


def write_binary_file(
    filepath: str,
    features: np.ndarray,
    labels: np.ndarray,
    binned: np.ndarray,
    bin_edges: np.ndarray,
):
    """
    Write dataset to binary file for the standalone Rust binary.

    Format:
      [num_rows: u32] [num_features: u16]
      [labels: N × u8]
      [binned features column-major: 28 × N × u8]
      [bin_edges: 28 × 255 × f32]
    """
    num_rows, num_features = features.shape
    print(f"  Writing binary file: {filepath}")

    with open(filepath, "wb") as f:
        f.write(struct.pack("<I", num_rows))
        f.write(struct.pack("<H", num_features))
        f.write(labels.tobytes())
        # Column-major binned features
        for col in range(num_features):
            f.write(binned[:, col].tobytes())
        # Bin edges
        f.write(bin_edges.tobytes())

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Written {filepath} ({size_mb:.1f} MB)")


def write_s3_partitions(
    features: np.ndarray,
    labels: np.ndarray,
    binned: np.ndarray,
    bin_edges: np.ndarray,
    num_partitions: int,
    bucket: str,
    s3_prefix: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
):
    """
    Upload partitioned dataset to S3 for burst workers.

    Each partition contains a subset of rows (row_id % num_partitions == partition_id).
    Same binary format as local file but per-partition subset.
    Bin edges uploaded once as a shared object.
    """
    num_rows, num_features = features.shape
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
        pass

    # Upload bin edges (shared across all workers)
    edges_key = f"{s3_prefix}/bin_edges"
    s3.put_object(
        Bucket=bucket,
        Key=edges_key,
        Body=bin_edges.tobytes(),
        ContentType="application/octet-stream",
    )
    print(f"    ✅ Bin edges: {edges_key} ({bin_edges.nbytes / 1024:.1f} KB)")

    # Upload metadata
    meta = {
        "num_rows": int(num_rows),
        "num_features": int(num_features),
        "num_bins": NUM_BINS,
        "num_partitions": num_partitions,
    }
    import json
    meta_key = f"{s3_prefix}/metadata.json"
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta).encode("utf-8"),
        ContentType="application/json",
    )

    # Partition rows
    for p in range(num_partitions):
        row_mask = np.arange(num_rows) % num_partitions == p
        part_labels = labels[row_mask]
        part_binned = binned[row_mask]
        part_n = int(row_mask.sum())

        buf = io.BytesIO()
        buf.write(struct.pack("<I", part_n))
        buf.write(struct.pack("<H", num_features))
        buf.write(part_labels.tobytes())
        for col in range(num_features):
            buf.write(part_binned[:, col].tobytes())

        key = f"{s3_prefix}/part-{p:05d}"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"    ✅ Partition {p}: {part_n:,} rows ({len(buf.getvalue()) / 1024:.0f} KB)")


def generate_gb_dataset(
    num_rows: int,
    num_partitions: int,
    output_local: str,
    bucket: str | None = None,
    s3_prefix: str | None = None,
    endpoint: str = "localhost:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    seed: int = 42,
):
    """Generate Gradient Boosting dataset (local file + optional S3 partitions)."""
    print(f"Generating GB dataset: {num_rows:,} rows, seed={seed}...")

    cache = download_higgs()
    full_features, full_labels = load_higgs(cache, max_rows=max(num_rows + 100_000, num_rows))

    if num_rows < len(full_labels):
        features, labels = create_subset(full_features, full_labels, num_rows, seed)
    else:
        features, labels = full_features[:num_rows], full_labels[:num_rows]

    print(f"  Class distribution: {np.sum(labels == 0):,} (0) / {np.sum(labels == 1):,} (1)")

    # Compute bin edges and digitize
    bin_edges = compute_bin_edges(features)
    binned = digitize_features(features, bin_edges)

    # Write local file
    write_binary_file(output_local, features, labels, binned, bin_edges)

    # Upload to S3
    if bucket and s3_prefix:
        write_s3_partitions(
            features, labels, binned, bin_edges,
            num_partitions, bucket, s3_prefix,
            endpoint, access_key, secret_key,
        )

    print(f"✅ GB dataset ready: {num_rows:,} rows × {NUM_FEATURES} features")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate Gradient Boosting datasets from Higgs (standalone + S3 partitions)"
    )
    parser.add_argument("--rows", type=int, required=True, help="Number of rows")
    parser.add_argument("--partitions", type=int, default=8, help="Number of S3 partitions")
    parser.add_argument("--output", type=str, default=None, help="Local output file path")
    parser.add_argument("--bucket", type=str, default="test-bucket", help="S3 bucket name")
    parser.add_argument("--prefix", type=str, default=None, help="S3 key prefix")
    parser.add_argument("--endpoint", type=str, default="localhost:9000", help="S3 endpoint URL")
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 upload (local file only)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")

    args = parser.parse_args()

    if args.output is None:
        args.output = f"large_gb_{args.rows}.bin"
    if args.prefix is None:
        args.prefix = f"datasets/higgs-{args.rows}"

    generate_gb_dataset(
        num_rows=args.rows,
        num_partitions=args.partitions,
        output_local=args.output,
        bucket=None if args.no_s3 else args.bucket,
        s3_prefix=None if args.no_s3 else args.prefix,
        endpoint=args.endpoint,
        seed=args.seed,
    )
