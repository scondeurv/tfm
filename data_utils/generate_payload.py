import argparse
import json
from pprint import pprint


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate input for the Label Propagation application on OpenWhisk Burst")
    parser.add_argument(
        "--partitions", type=int, required=True, help="Number of dataset partitions (burst size, num of workers)"
    )
    parser.add_argument("--num_nodes", type=int, required=True,
                        help="Number of nodes in the dataset graph")
    parser.add_argument("--convergence_threshold", type=int, default=0,
                            help="Stop when total changed labels <= threshold (default 0)")
    parser.add_argument("--max_iterations", type=int, default=0, 
                            help="Number of iterations, overrides default (50)")
    parser.add_argument("--s3_bucket", type=str,
                        required=True, help="S3 bucket name")
    parser.add_argument("--s3_key", type=str,
                        required=True, help="S3 key prefix")
    parser.add_argument("--s3_endpoint", type=str,
                        default="", help="S3 endpoint URL")
    parser.add_argument("--s3_region", type=str,
                        required=True, help="S3 region name")
    parser.add_argument("--aws_access_key_id", type=str,
                        required=True, help="AWS access key ID")
    parser.add_argument("--aws_secret_access_key", type=str,
                        required=True, help="AWS secret access key")
    parser.add_argument(
        "--output", type=str, default="labelpropagation_payload.json", help="Output file path")
    parser.add_argument("--split", type=int, default=1,
                        help="Split output file")
    args = parser.parse_args()

    pprint(args)

    payload_list = []
    for i in range(args.partitions):
        payload = {
            "num_nodes": args.num_nodes,
            "convergence_threshold": args.convergence_threshold,
            "input_data": {
                "bucket": args.s3_bucket,
                "key": f"{args.s3_key}/part-{str(i).zfill(5)}",
                "endpoint": args.s3_endpoint,
                "region": args.s3_region,
                "aws_access_key_id": args.aws_access_key_id,
                "aws_secret_access_key": args.aws_secret_access_key,
            },
        }

        if args.max_iterations:
            payload["max_iterations"] = args.max_iterations
        if not args.s3_endpoint:
            del payload["input_data"]["endpoint"]

        payload_list.append(payload)

    if args.split == 1:
        with open(args.output, "w") as f:
            f.write(json.dumps(payload_list, indent=4))
    else:
        assert args.partitions % args.split == 0
        sz = args.partitions // args.split
        for i in range(args.split):
            with open(f"part-{str(i).zfill(4)}_" + args.output, "w") as f:
                f.write(json.dumps(
                    payload_list[sz * i:(sz * i) + sz], indent=4))
