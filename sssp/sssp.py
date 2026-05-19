#!/usr/bin/env python3
"""
Direct SSSP burst launcher (single run, saves result to sssp-burst.json).

Mirror of bfs.py for the SSSP benchmark.
"""
import argparse
import json

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from sssp_utils import generate_sssp_payload, add_sssp_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch SSSP burst action (single run)")
    add_openwhisk_to_parser(parser)
    add_sssp_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    effective_granularity = args.granularity or 1
    params = generate_sssp_payload(
        endpoint=args.sssp_endpoint,
        partitions=args.partitions,
        num_nodes=args.num_nodes,
        bucket=args.bucket,
        key=args.key,
        source_node=args.source_node,
        max_iterations=args.max_iterations,
        granularity=effective_granularity,
    )

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    host_submit = get_millis()
    dt = executor.burst(
        "sssp",
        params,
        file="./sssp.zip",
        memory=args.runtime_memory if args.runtime_memory else 2048,
        custom_image=args.custom_image,
        debug_mode=args.debug,
        granularity=effective_granularity,
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

    with open("sssp-burst.json", "w") as f:
        json.dump(flattened_results, f, indent=2)

    print(f"✅ Results saved to sssp-burst.json  (wall-clock: {finished - host_submit} ms)")
