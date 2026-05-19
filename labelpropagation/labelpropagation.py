import argparse
import pandas as pd
import json

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from labelpropagation_utils import generate_payload, add_labelpropagation_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_labelpropagation_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    effective_granularity = args.granularity or 1
    params = generate_payload(
        endpoint=args.lp_endpoint,
        partitions=args.partitions,
        num_nodes=args.num_nodes,
        bucket=args.bucket,
        key=args.key,
        convergence_threshold=args.convergence_threshold,
        max_iterations=args.max_iterations,
        granularity=effective_granularity
    )

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    host_submit = get_millis()
    dt = executor.burst("labelpropagation",
                        params,
                        file="./labelpropagation.zip",
                        memory=args.runtime_memory if args.runtime_memory else 2048,
                        custom_image=args.custom_image,
                        debug_mode=args.debug,
                        granularity=effective_granularity,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True,
                        timeout=300000)
    finished = get_millis()

    dt_results = dt.get_results()
    
    flattened_results = []
    for sublist in dt_results:
        if isinstance(sublist, list):
            flattened_results.extend(sublist)
        else:
            # Handle error case or unexpected structure
            print(f"WARNING: Unexpected result structure: {sublist}")
            continue

    if not flattened_results:
        print("ERROR: No results returned from workers.")
        exit(1)

    flattened_results.sort(key=lambda x: x.get('key', 'unknown'))

    results = []
    for i in flattened_results:
        res = {
            "fn_id": i["key"],
            "host_submit": host_submit,
            "timestamps": i["timestamps"],
            "finished": finished
        }
        if "labels" in i:
            res["labels"] = i["labels"]
        results.append(res)

    json.dump(results, open("labelpropagation-burst.json", "w"))
