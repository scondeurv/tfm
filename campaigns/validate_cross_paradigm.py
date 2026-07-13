#!/usr/bin/env python3
"""Cross-paradigm correctness gate for the clean campaign.

Compares a candidate paradigm's output vector against the standalone golden for
the same graph. This is the Phase-1 spine: no paradigm advances to timing until
it matches standalone on a small graph.

Output vectors (index = node id):
  lp        -> "labels"     (u32)   compared as a PARTITION (equiv up to relabel)
  bfs       -> "levels"     (u32)   exact; UNREACHABLE sentinel normalized
  sssp      -> "distances"  (f64)   relative tol (float reduction order differs)
  pagerank  -> "rank"       (f64)   relative tol

Golden/candidate may be:
  - a native stdout JSON blob (dict with the vector field), file or stdin
  - a burst S3 object (labels_final.json / levels_final.json / ...), via --s3
Each source is normalized to a python list indexed by node id.

CLI:
  validate_cross_paradigm.py --algo lp --golden std.json --candidate rayon.json
  echo "$JSON" | validate_cross_paradigm.py --algo bfs --golden - --candidate mpi.json
Exit 0 = match, 1 = mismatch, 2 = setup error.
"""
import argparse
import json
import math
import os
import sys

# u32::MAX sentinels used by the kernels for "unreachable" / "unknown".
U32_MAX = 2**32 - 1
UNREACHABLE_SENTINELS = {U32_MAX, U32_MAX - 0, -1}

VECTOR_FIELD = {
    "lp": ("labels", "labels_final"),
    "bfs": ("levels", "levels_final"),
    "sssp": ("distances", "distances_final"),
    "pagerank": ("rank", "rank_final", "ranks"),
}


def extract_vector(raw, algo, *, source):
    """Return a list indexed by node id from a native-JSON dict or a bare list/obj."""
    fields = VECTOR_FIELD[algo]
    payload = raw
    if isinstance(raw, dict):
        payload = None
        for f in fields:
            if f in raw:
                payload = raw[f]
                break
        if payload is None:
            # burst objects sometimes wrap under "labels"/"result"
            for f in ("labels", "result", "values", "vector"):
                if f in raw:
                    payload = raw[f]
                    break
        if payload is None:
            raise ValueError(f"{source}: no vector field {fields} in keys {list(raw.keys())[:8]}")
    if isinstance(payload, dict):
        # {node: value} -> dense list
        n = max(int(k) for k in payload) + 1
        vec = [None] * n
        for k, v in payload.items():
            vec[int(k)] = v
        return vec
    if isinstance(payload, list):
        return payload
    raise ValueError(f"{source}: vector must be list or object, got {type(payload)}")


def load_source(spec, algo, *, source):
    """spec: '-' stdin, a path, or 's3:bucket/key' (needs boto3 + AWS env)."""
    if spec == "-":
        raw = json.load(sys.stdin)
    elif spec.startswith("s3:"):
        raw = _load_s3(spec[3:], algo)
    else:
        with open(spec, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    return extract_vector(raw, algo, source=source)


def _load_s3(bucket_key, algo):
    import boto3
    from botocore.config import Config

    bucket, key = bucket_key.split("/", 1)
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    if not endpoint.startswith("http"):
        endpoint = "http://" + endpoint
    s3 = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return json.loads(body)


# ---- comparators -----------------------------------------------------------

def _norm_unreachable(v):
    return None if (isinstance(v, int) and v in UNREACHABLE_SENTINELS) else v


def compare_exact(golden, cand, max_ex=20):
    """BFS levels: exact per-node int match, unreachable sentinels unified."""
    if len(golden) != len(cand):
        return False, [f"length {len(golden)} vs {len(cand)}"]
    ex = []
    for i, (g, c) in enumerate(zip(golden, cand)):
        if _norm_unreachable(g) != _norm_unreachable(c):
            ex.append(f"node {i}: {g} vs {c}")
            if len(ex) >= max_ex:
                break
    return (not ex), ex


def compare_tol(golden, cand, rel=1e-6, abs_=1e-9, max_ex=20):
    """SSSP/PR floats: relative tolerance, unreachable (inf/sentinel) unified."""
    if len(golden) != len(cand):
        return False, [f"length {len(golden)} vs {len(cand)}"]
    ex = []
    INF = float("inf")
    for i, (g, c) in enumerate(zip(golden, cand)):
        gf = INF if (g is None or (isinstance(g, (int, float)) and g >= U32_MAX)) else float(g)
        cf = INF if (c is None or (isinstance(c, (int, float)) and c >= U32_MAX)) else float(c)
        if gf == INF or cf == INF:
            if gf != cf:
                ex.append(f"node {i}: {g} vs {c} (reachability)")
        elif not math.isclose(gf, cf, rel_tol=rel, abs_tol=abs_):
            ex.append(f"node {i}: {gf} vs {cf}")
        if len(ex) >= max_ex:
            break
    return (not ex), ex


def compare_partition(golden, cand, max_ex=20):
    """LP labels: same induced partition (bijection between label sets)."""
    if len(golden) != len(cand):
        return False, [f"length {len(golden)} vs {len(cand)}"]
    fwd, rev = {}, {}
    ex = []
    for i, (g, c) in enumerate(zip(golden, cand)):
        if g in fwd:
            if fwd[g] != c:
                ex.append(f"node {i}: golden-label {g} maps to {fwd[g]} but sees {c}")
        else:
            fwd[g] = c
        if c in rev:
            if rev[c] != g:
                ex.append(f"node {i}: cand-label {c} already bound to {rev[c]} not {g}")
        else:
            rev[c] = g
        if len(ex) >= max_ex:
            break
    return (not ex), ex


def compare(algo, golden, cand, rel=1e-6):
    if algo == "lp":
        return compare_partition(golden, cand)
    if algo == "bfs":
        return compare_exact(golden, cand)
    if algo in ("sssp", "pagerank"):
        return compare_tol(golden, cand, rel=rel)
    raise ValueError(f"unknown algo {algo}")


def main():
    ap = argparse.ArgumentParser(description="Cross-paradigm correctness gate")
    ap.add_argument("--algo", required=True, choices=list(VECTOR_FIELD))
    ap.add_argument("--golden", required=True, help="standalone golden: path, '-' stdin, or s3:bucket/key")
    ap.add_argument("--candidate", required=True, help="paradigm output: path, '-' stdin, or s3:bucket/key")
    ap.add_argument("--rel-tol", type=float, default=1e-6)
    ap.add_argument("--label", default="", help="tag printed in the pass/fail line")
    args = ap.parse_args()

    try:
        golden = load_source(args.golden, args.algo, source="golden")
        cand = load_source(args.candidate, args.algo, source="candidate")
    except Exception as exc:
        print(f"[gate] SETUP-FAIL {args.algo} {args.label}: {exc}", file=sys.stderr)
        return 2

    ok, examples = compare(args.algo, golden, cand, rel=args.rel_tol)
    tag = f"{args.algo} {args.label}".strip()
    if ok:
        print(f"[gate] PASS {tag}: {len(golden)} nodes match")
        return 0
    print(f"[gate] FAIL {tag}: {len(examples)} sample diffs", file=sys.stderr)
    for e in examples:
        print(f"    {e}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
