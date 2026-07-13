#!/usr/bin/env python3
import argparse
import hashlib
import heapq
import json
import math
import os
from collections import deque

import boto3
from botocore.config import Config


def s3_client(endpoint: str):
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def load_json_from_s3(bucket: str, key: str, endpoint: str):
    obj = s3_client(endpoint).get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def read_edges(path: str, weighted: bool):
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src = int(parts[0])
            dst = int(parts[1])
            weight = float(parts[2]) if weighted and len(parts) >= 3 else 1.0
            yield src, dst, weight


def validate_bfs(args) -> bool:
    adj = [[] for _ in range(args.nodes)]
    for src, dst, _ in read_edges(args.graph_file, weighted=False):
        if 0 <= src < args.nodes and 0 <= dst < args.nodes:
            adj[src].append(dst)

    levels = [-1] * args.nodes
    levels[args.source] = 0
    queue = deque([args.source])
    while queue:
        node = queue.popleft()
        if levels[node] >= args.max_levels:
            continue
        for dst in adj[node]:
            if levels[dst] == -1:
                levels[dst] = levels[node] + 1
                queue.append(dst)

    payload = load_json_from_s3(
        args.bucket, f"{args.key_prefix}/output/bfs_levels_final.json", args.endpoint
    )
    burst_levels = [
        -1 if value == 0xFFFF_FFFF else value
        for value in (payload.get("levels") or [])
    ]
    if levels != burst_levels:
        for idx, (expected, actual) in enumerate(zip(levels, burst_levels or [])):
            if expected != actual:
                print(f"BFS mismatch node={idx} expected={expected} burst={actual}")
                break
        print("BFS VALIDATION FAILED")
        return False

    print(
        f"BFS VALIDATION PASSED nodes={args.nodes} visited={sum(1 for v in levels if v >= 0)} "
        f"max_level={max(levels)}"
    )
    return True


def validate_sssp(args) -> bool:
    adj = [[] for _ in range(args.nodes)]
    for src, dst, weight in read_edges(args.graph_file, weighted=True):
        if 0 <= src < args.nodes and 0 <= dst < args.nodes:
            adj[src].append((dst, weight))

    dist = [math.inf] * args.nodes
    dist[args.source] = 0.0
    heap = [(0.0, args.source)]
    while heap:
        cur, node = heapq.heappop(heap)
        if cur != dist[node]:
            continue
        for dst, weight in adj[node]:
            cand = cur + weight
            if cand < dist[dst]:
                dist[dst] = cand
                heapq.heappush(heap, (cand, dst))

    payload = load_json_from_s3(
        args.bucket, f"{args.key_prefix}/output/sssp_distances_final.json", args.endpoint
    )
    burst_dist = payload.get("distances")
    if not isinstance(burst_dist, list) or len(burst_dist) != len(dist):
        print("SSSP VALIDATION FAILED: missing or invalid burst distance vector")
        return False

    for idx, (expected, actual_raw) in enumerate(zip(dist, burst_dist)):
        actual = math.inf if actual_raw is None else float(actual_raw)
        if math.isinf(expected) and math.isinf(actual):
            continue
        tol = max(abs(expected) * 1e-4, 1e-3)
        if abs(expected - actual) > tol:
            print(f"SSSP mismatch node={idx} expected={expected} burst={actual_raw} tol={tol}")
            print("SSSP VALIDATION FAILED")
            return False

    reachable = sum(1 for value in dist if math.isfinite(value))
    max_dist = max((value for value in dist if math.isfinite(value)), default=0.0)
    print(f"SSSP VALIDATION PASSED nodes={args.nodes} reachable={reachable} max_dist={max_dist:.4f}")
    return True


class UnionFind:
    def __init__(self, size: int):
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, node: int) -> int:
        while self.parent[node] != node:
            self.parent[node] = self.parent[self.parent[node]]
            node = self.parent[node]
        return node

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if self.rank[root_left] < self.rank[root_right]:
            self.parent[root_left] = root_right
        elif self.rank[root_left] > self.rank[root_right]:
            self.parent[root_right] = root_left
        else:
            self.parent[root_right] = root_left
            self.rank[root_left] += 1


def canonical_component_hash(labels: list[int]) -> str:
    root_to_component = {}
    next_component = 0
    hash_value = 0xCBF29CE484222325
    for root in labels:
        if root not in root_to_component:
            root_to_component[root] = next_component
            next_component += 1
        component = root_to_component[root]
        hash_value ^= component
        hash_value = (hash_value * 0x100000001B3) & 0xFFFF_FFFF_FFFF_FFFF
    return f"{hash_value:016x}"


def validate_wcc(args) -> bool:
    uf = UnionFind(args.nodes)
    edge_count = 0
    for src, dst, _ in read_edges(args.graph_file, weighted=False):
        if 0 <= src < args.nodes and 0 <= dst < args.nodes:
            uf.union(src, dst)
            edge_count += 1

    roots = [uf.find(node) for node in range(args.nodes)]
    expected_components = len(set(roots))
    expected_hash = canonical_component_hash(roots)

    if args.expected_components is not None and expected_components != args.expected_components:
        print(
            "WCC VALIDATION FAILED: "
            f"expected_components={expected_components} burst_components={args.expected_components}"
        )
        return False
    if args.expected_hash and expected_hash != args.expected_hash:
        print(
            "WCC VALIDATION FAILED: "
            f"expected_hash={expected_hash} burst_hash={args.expected_hash}"
        )
        return False

    print(
        f"WCC VALIDATION PASSED nodes={args.nodes} edges={edge_count} "
        f"components={expected_components} component_hash={expected_hash}"
    )
    return True


UNKNOWN = 0xFFFF_FFFF


def majority_label(counts: dict[int, int], current: int) -> int:
    if not counts:
        return current
    best = current
    best_count = 0
    for label, count in counts.items():
        if label == UNKNOWN:
            continue
        if count > best_count or (count == best_count and label < best):
            best = label
            best_count = count
    return best


def validate_lp(args) -> bool:
    adj = [[] for _ in range(args.nodes)]
    initial_labels = {}
    for src, dst, weight in read_edges(args.graph_file, weighted=True):
        if 0 <= src < args.nodes and 0 <= dst < args.nodes:
            adj[src].append(dst)
            if not math.isclose(weight, 1.0):
                initial_labels[src] = int(weight)

    unsupervised = not initial_labels
    labels = [UNKNOWN] * args.nodes
    if unsupervised:
        labels = list(range(args.nodes))
    else:
        for node, label in initial_labels.items():
            labels[node] = label

    for _ in range(args.max_iterations):
        prev = labels.copy()
        changed = 0
        for node in range(args.nodes):
            if not unsupervised and node in initial_labels:
                continue
            counts = {}
            for neighbor in adj[node]:
                label = prev[neighbor]
                if label != UNKNOWN:
                    counts[label] = counts.get(label, 0) + 1
            new_label = majority_label(counts, prev[node])
            if new_label != prev[node]:
                labels[node] = new_label
                changed += 1
        if changed == 0:
            break

    payload = load_json_from_s3(
        args.bucket, f"{args.key_prefix}/output/labels_final.json", args.endpoint
    )
    burst_labels_obj = payload.get("labels") if isinstance(payload, dict) else None
    if not isinstance(burst_labels_obj, dict):
        print("LP VALIDATION FAILED: missing labels object")
        return False
    burst_labels = [int(burst_labels_obj.get(str(node), UNKNOWN)) for node in range(args.nodes)]

    if labels != burst_labels:
        for idx, (expected, actual) in enumerate(zip(labels, burst_labels)):
            if expected != actual:
                print(f"LP mismatch node={idx} expected={expected} burst={actual}")
                break
        print("LP VALIDATION FAILED")
        return False

    digest = hashlib.sha256(",".join(str(v) for v in labels).encode("utf-8")).hexdigest()[:16]
    print(f"LP VALIDATION PASSED nodes={args.nodes} labels_hash={digest}")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--algorithm", choices=["bfs", "sssp", "wcc", "lp"], required=True)
    parser.add_argument("--graph-file", required=True)
    parser.add_argument("--nodes", type=int, required=True)
    parser.add_argument("--source", type=int, default=0)
    parser.add_argument("--max-levels", type=int, default=500)
    parser.add_argument("--max-iterations", type=int, default=20)
    parser.add_argument("--bucket")
    parser.add_argument("--key-prefix")
    parser.add_argument("--endpoint")
    parser.add_argument("--expected-components", type=int)
    parser.add_argument("--expected-hash")
    args = parser.parse_args()

    if args.algorithm in {"bfs", "sssp", "lp"}:
        missing = [name for name in ("bucket", "key_prefix", "endpoint") if getattr(args, name) is None]
        if missing:
            parser.error(f"--algorithm {args.algorithm} requires: {', '.join('--' + m.replace('_', '-') for m in missing)}")

    if args.algorithm == "bfs":
        ok = validate_bfs(args)
    elif args.algorithm == "sssp":
        ok = validate_sssp(args)
    elif args.algorithm == "wcc":
        ok = validate_wcc(args)
    else:
        ok = validate_lp(args)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
