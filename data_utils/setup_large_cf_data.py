#!/usr/bin/env python3
"""
Download and prepare MovieLens dataset for Collaborative Filtering benchmarking.

Creates both a local binary file for the standalone binary and S3 partitions
for the burst worker.  The MovieLens 25M dataset (~25M ratings, ~162K users,
~62K movies) is a standard benchmark for collaborative filtering algorithms.

Data format (binary, little-endian, CSR):
  Header:   [num_users: u32] [num_items: u32] [num_ratings: u64]
  row_ptr:  [(num_users + 1) × u64]   – CSR row pointers
  col_idx:  [num_ratings × u32]        – item column indices
  values:   [num_ratings × f32]        – rating values

S3 partition format (per partition):
  Header:   [num_users: u32] [num_items: u32] [num_ratings: u64] [user_offset: u32]
  row_ptr:  [(partition_users + 1) × u64]
  col_idx:  [partition_ratings × u32]
  values:   [partition_ratings × f32]
"""
import argparse
import io
import os
import struct
import zipfile
import urllib.request

import numpy as np
import boto3
from botocore.client import Config

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
MOVIELENS_CACHE = "ml-25m.zip"


def download_movielens(cache_path: str = MOVIELENS_CACHE) -> str:
    """Download MovieLens 25M dataset if not already cached."""
    if os.path.exists(cache_path):
        print(f"  Using cached MovieLens dataset: {cache_path}")
        return cache_path

    print(f"  Downloading MovieLens 25M...")
    print(f"  URL: {MOVIELENS_URL}")
    print(f"  This may take a few minutes (~250 MB compressed)...")

    urllib.request.urlretrieve(MOVIELENS_URL, cache_path)
    print(f"  ✅ Downloaded to {cache_path}")
    return cache_path


def load_movielens(
    cache_path: str, max_users: int | None = None
) -> tuple[np.ndarray, np.ndarray, np.ndarray, int, int]:
    """
    Load MovieLens ratings CSV from zip into numpy arrays.

    Returns (user_ids, item_ids, ratings, num_users, num_items) where:
      user_ids: shape (N,), dtype int32 — remapped to 0-based contiguous
      item_ids: shape (N,), dtype int32 — remapped to 0-based contiguous
      ratings:  shape (N,), dtype float32
    """
    print(f"  Loading ratings from {cache_path}...")

    users = []
    items = []
    vals = []

    with zipfile.ZipFile(cache_path, "r") as zf:
        with zf.open("ml-25m/ratings.csv") as f:
            f.readline()  # skip header
            for line_bytes in f:
                line = line_bytes.decode("utf-8").strip()
                parts = line.split(",")
                if len(parts) < 3:
                    continue
                users.append(int(parts[0]))
                items.append(int(parts[1]))
                vals.append(float(parts[2]))
                if len(vals) % 5_000_000 == 0:
                    print(f"    Loaded {len(vals):,} ratings...")

    print(f"  ✅ Loaded {len(vals):,} raw ratings")

    user_arr = np.array(users, dtype=np.int64)
    item_arr = np.array(items, dtype=np.int64)
    val_arr = np.array(vals, dtype=np.float32)

    # Remap user/item IDs to contiguous 0-based
    unique_users = np.unique(user_arr)
    unique_items = np.unique(item_arr)

    if max_users and max_users < len(unique_users):
        # Take the first max_users unique user IDs
        rng = np.random.default_rng(42)
        chosen_users = set(rng.choice(unique_users, size=max_users, replace=False))
        mask = np.array([u in chosen_users for u in user_arr])
        user_arr = user_arr[mask]
        item_arr = item_arr[mask]
        val_arr = val_arr[mask]
        unique_users = np.unique(user_arr)
        unique_items = np.unique(item_arr)
        print(f"  Subset: {max_users} users → {len(val_arr):,} ratings, {len(unique_items)} items")

    user_map = {uid: idx for idx, uid in enumerate(unique_users)}
    item_map = {iid: idx for idx, iid in enumerate(unique_items)}

    remapped_users = np.array([user_map[u] for u in user_arr], dtype=np.int32)
    remapped_items = np.array([item_map[i] for i in item_arr], dtype=np.int32)

    num_users = len(unique_users)
    num_items = len(unique_items)
    print(f"  Final: {num_users:,} users × {num_items:,} items = {len(val_arr):,} ratings")

    return remapped_users, remapped_items, val_arr, num_users, num_items


def generate_synthetic(
    num_users: int,
    num_items: int,
    avg_ratings_per_user: int = 150,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Generate synthetic sparse ratings data.

    Each user rates avg_ratings_per_user items with ratings in [0.5, 5.0].
    """
    rng = np.random.default_rng(seed)
    print(f"  Generating synthetic: {num_users:,} users × {num_items:,} items, "
          f"~{avg_ratings_per_user} ratings/user...")

    all_users = []
    all_items = []
    all_vals = []

    for u in range(num_users):
        n_ratings = rng.poisson(avg_ratings_per_user)
        n_ratings = min(max(n_ratings, 1), num_items)
        rated_items = rng.choice(num_items, size=n_ratings, replace=False)
        ratings = rng.choice([0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0], size=n_ratings)

        all_users.extend([u] * n_ratings)
        all_items.extend(rated_items.tolist())
        all_vals.extend(ratings.tolist())

        if (u + 1) % 100_000 == 0:
            print(f"    Generated user {u + 1:,}/{num_users:,}...")

    user_arr = np.array(all_users, dtype=np.int32)
    item_arr = np.array(all_items, dtype=np.int32)
    val_arr = np.array(all_vals, dtype=np.float32)

    print(f"  ✅ Generated {len(val_arr):,} ratings")
    return user_arr, item_arr, val_arr


def build_csr(
    users: np.ndarray,
    items: np.ndarray,
    values: np.ndarray,
    num_users: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Build CSR arrays from COO triplets, sorted by user then item.

    Returns (row_ptr, col_idx, vals).
    """
    # Sort by (user, item)
    order = np.lexsort((items, users))
    users = users[order]
    items = items[order]
    values = values[order]

    row_ptr = np.zeros(num_users + 1, dtype=np.uint64)
    for u in users:
        row_ptr[u + 1] += 1
    row_ptr = np.cumsum(row_ptr)

    col_idx = items.astype(np.uint32)
    vals = values.astype(np.float32)

    return row_ptr, col_idx, vals


def write_binary_file(
    filepath: str,
    row_ptr: np.ndarray,
    col_idx: np.ndarray,
    values: np.ndarray,
    num_users: int,
    num_items: int,
):
    """
    Write CSR dataset to binary file for the standalone Rust binary.

    Format:
      [num_users: u32] [num_items: u32] [num_ratings: u64]
      [row_ptr: (num_users+1) × u64]
      [col_idx: num_ratings × u32]
      [values:  num_ratings × f32]
    """
    num_ratings = len(col_idx)
    print(f"  Writing binary file: {filepath}")

    with open(filepath, "wb") as f:
        f.write(struct.pack("<I", num_users))
        f.write(struct.pack("<I", num_items))
        f.write(struct.pack("<Q", num_ratings))
        f.write(row_ptr.tobytes())
        f.write(col_idx.tobytes())
        f.write(values.tobytes())

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Written {filepath} ({size_mb:.1f} MB)")


def write_s3_partitions(
    row_ptr: np.ndarray,
    col_idx: np.ndarray,
    values: np.ndarray,
    num_users: int,
    num_items: int,
    num_partitions: int,
    bucket: str,
    s3_prefix: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
):
    """
    Upload partitioned CSR dataset to S3 for burst workers.

    Each partition contains a contiguous block of users:
      partition i gets users [start_user, end_user).
    The partition binary includes a user_offset field.
    """
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

    # Upload metadata
    import json
    meta = {
        "num_users": num_users,
        "num_items": num_items,
        "num_ratings": int(row_ptr[-1]),
        "num_partitions": num_partitions,
    }
    meta_key = f"{s3_prefix}/metadata.json"
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta).encode("utf-8"),
        ContentType="application/json",
    )

    # Split users into contiguous blocks
    users_per_part = num_users // num_partitions
    remainder = num_users % num_partitions

    user_offset = 0
    for p in range(num_partitions):
        part_users = users_per_part + (1 if p < remainder else 0)
        start_user = user_offset
        end_user = user_offset + part_users

        # Extract CSR slice for this user range
        part_row_ptr = row_ptr[start_user: end_user + 1].copy()
        nnz_start = int(part_row_ptr[0])
        nnz_end = int(part_row_ptr[-1])
        part_col_idx = col_idx[nnz_start:nnz_end]
        part_values = values[nnz_start:nnz_end]

        # Rebase row_ptr to start from 0
        part_row_ptr = (part_row_ptr - part_row_ptr[0]).astype(np.uint64)
        part_nnz = int(part_row_ptr[-1])

        buf = io.BytesIO()
        buf.write(struct.pack("<I", part_users))
        buf.write(struct.pack("<I", num_items))
        buf.write(struct.pack("<Q", part_nnz))
        buf.write(struct.pack("<I", start_user))  # user_offset
        buf.write(part_row_ptr.tobytes())
        buf.write(part_col_idx.astype(np.uint32).tobytes())
        buf.write(part_values.astype(np.float32).tobytes())

        key = f"{s3_prefix}/part-{p:05d}"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"    ✅ Partition {p}: users [{start_user}..{end_user}) "
              f"({part_users:,} users, {part_nnz:,} ratings, "
              f"{len(buf.getvalue()) / 1024:.0f} KB)")

        user_offset = end_user


def generate_cf_dataset(
    num_users: int | None = None,
    num_items: int | None = None,
    num_partitions: int = 4,
    output_local: str = "large_cf.bin",
    bucket: str | None = None,
    s3_prefix: str | None = None,
    endpoint: str = "localhost:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    seed: int = 42,
    synthetic: bool = False,
    avg_ratings_per_user: int = 150,
):
    """Generate CF dataset (local file + optional S3 partitions)."""
    if synthetic:
        assert num_users is not None and num_items is not None
        print(f"Generating synthetic CF dataset: {num_users:,} users × {num_items:,} items...")
        users, items, vals = generate_synthetic(num_users, num_items, avg_ratings_per_user, seed)
        actual_users = num_users
        actual_items = num_items
    else:
        print("Generating CF dataset from MovieLens 25M...")
        cache = download_movielens()
        users, items, vals, actual_users, actual_items = load_movielens(
            cache, max_users=num_users
        )

    print(f"  Building CSR ({actual_users:,} users, {actual_items:,} items, "
          f"{len(vals):,} ratings)...")
    row_ptr, col_idx, values = build_csr(users, items, vals, actual_users)

    # Write local file
    write_binary_file(output_local, row_ptr, col_idx, values, actual_users, actual_items)

    # Upload to S3
    if bucket and s3_prefix:
        write_s3_partitions(
            row_ptr, col_idx, values,
            actual_users, actual_items,
            num_partitions, bucket, s3_prefix,
            endpoint, access_key, secret_key,
        )

    print(f"✅ CF dataset ready: {actual_users:,} users × {actual_items:,} items "
          f"= {len(vals):,} ratings")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate CF datasets from MovieLens or synthetic (standalone + S3 partitions)"
    )
    parser.add_argument("--users", type=int, default=None,
                        help="Number of users (None = all MovieLens users)")
    parser.add_argument("--items", type=int, default=None,
                        help="Number of items (required for synthetic)")
    parser.add_argument("--partitions", type=int, default=4, help="Number of S3 partitions")
    parser.add_argument("--output", type=str, default=None, help="Local output file path")
    parser.add_argument("--bucket", type=str, default="test-bucket", help="S3 bucket name")
    parser.add_argument("--prefix", type=str, default=None, help="S3 key prefix")
    parser.add_argument("--endpoint", type=str, default="localhost:9000", help="S3 endpoint URL")
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 upload (local file only)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    parser.add_argument("--synthetic", action="store_true",
                        help="Generate synthetic data instead of using MovieLens")
    parser.add_argument("--avg-ratings", type=int, default=150,
                        help="Average ratings per user (synthetic only)")

    args = parser.parse_args()

    if args.output is None:
        if args.users:
            args.output = f"large_cf_{args.users}.bin"
        else:
            args.output = "large_cf_full.bin"
    if args.prefix is None:
        if args.users:
            args.prefix = f"datasets/cf-{args.users}"
        else:
            args.prefix = "datasets/cf-full"

    generate_cf_dataset(
        num_users=args.users,
        num_items=args.items,
        num_partitions=args.partitions,
        output_local=args.output,
        bucket=None if args.no_s3 else args.bucket,
        s3_prefix=None if args.no_s3 else args.prefix,
        endpoint=args.endpoint,
        seed=args.seed,
        synthetic=args.synthetic,
        avg_ratings_per_user=args.avg_ratings,
    )
