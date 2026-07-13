#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path("/home/sergio/src")
sys.path.insert(0, str(ROOT))

from baselines.run_spark_graph_benchmarks import (
    validate_bfs_spark_output,
    validate_lp_spark_output,
    validate_sssp_spark_output,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="algo", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--local-output-dir", type=Path, required=True)
    common.add_argument("--graph-file", type=Path, required=True)
    common.add_argument("--num-nodes", type=int, required=True)

    p_lp = sub.add_parser("lp", parents=[common])
    p_lp.add_argument("--max-iter", type=int, required=True)

    p_bfs = sub.add_parser("bfs", parents=[common])
    p_bfs.add_argument("--source-node", type=int, required=True)
    p_bfs.add_argument("--max-levels", type=int, required=True)

    p_sssp = sub.add_parser("sssp", parents=[common])
    p_sssp.add_argument("--source-node", type=int, required=True)

    args = parser.parse_args()

    if args.algo == "lp":
        summary = validate_lp_spark_output(
            graph_file=args.graph_file,
            num_nodes=args.num_nodes,
            max_iter=args.max_iter,
            spark_output_dir=args.local_output_dir,
        )
    elif args.algo == "bfs":
        summary = validate_bfs_spark_output(
            graph_file=args.graph_file,
            num_nodes=args.num_nodes,
            source_node=args.source_node,
            max_levels=args.max_levels,
            spark_output_dir=args.local_output_dir,
        )
    elif args.algo == "sssp":
        summary = validate_sssp_spark_output(
            graph_file=args.graph_file,
            num_nodes=args.num_nodes,
            source_node=args.source_node,
            spark_output_dir=args.local_output_dir,
        )
    else:
        raise SystemExit(f"unknown algo {args.algo}")

    summary.setdefault("algorithm", args.algo)
    print(json.dumps(summary))
    if not summary.get("passed", False):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
