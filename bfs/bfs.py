#!/usr/bin/env python3
"""
Direct BFS burst launcher (single run, saves result to bfs-burst.json).

Mirror of labelpropagation.py for the BFS benchmark.
"""
import argparse
import json

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from bfs_utils import generate_bfs_payload, add_bfs_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch BFS burst action (single run)")
    add_openwhisk_to_parser(parser)
    add_bfs_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    params = generate_bfs_payload(
        endpoint=args.bfs_endpoint,
        partitions=args.partitions,
        num_nodes=args.num_nodes,
        bucket=args.bucket,
        key=args.key,
        source_node=args.source_node,
        max_levels=args.max_levels,
        granularity=args.granularity,
    )

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    host_submit = get_millis()
    dt = executor.burst(
        "bfs",
        params,
        file="./bfs.zip",
        memory=args.runtime_memory if args.runtime_memory else 2048,
        custom_image=args.custom_image,
        debug_mode=args.debug,
        granularity=1,
        join=args.join,
        backend=args.backend,
        chunk_size=args.chunk_size,
        is_zip=True,
        timeout=600000,
    )
    finished = get_millis()

    dt_results = dt.get_results()

    flattened_results = []
    for sublist in dt_results:
        if isinstance(sublist, list):
            flattened_results.extend(sublist)
        else:
            flattened_results.append(sublist)

    flattened_results.sort(key=lambda x: x.get("key", ""))

    with open("bfs-burst.json", "w") as f:
        json.dump(flattened_results, f, indent=2)

    print(f"✅ Results saved to bfs-burst.json  (wall-clock: {finished - host_submit} ms)")
