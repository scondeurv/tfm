#!/usr/bin/env python3
"""
Generate large graph datasets for Label Propagation benchmarking.
Creates a local .txt graph file and, optionally, S3 partitions for Burst.
"""
import argparse
import io
import boto3
from botocore.client import Config


def delete_existing_partitions(s3, bucket: str, s3_prefix: str) -> None:
    paginator = s3.get_paginator("list_objects_v2")
    keys_to_delete: list[dict[str, str]] = []
    prefix = f"{s3_prefix}/part-"
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if key and key.startswith(prefix):
                keys_to_delete.append({"Key": key})

    for idx in range(0, len(keys_to_delete), 1000):
        batch = keys_to_delete[idx : idx + 1000]
        if batch:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})

def generate_large_graph(num_nodes, num_partitions, output_local, bucket=None, s3_prefix=None, 
                        endpoint="localhost:9000", access_key="minioadmin", secret_key="minioadmin",
                        density=10):
    """
    Generate a ring graph with seeds for Label Propagation.
    Each node connects to multiple neighbors (controlled by density).
    10% of nodes are seeded with labels based on their position.
    
    Args:
        density: Number of neighbors each node connects to (higher = denser graph)
    """
    print(f"Generating graph: {num_nodes} nodes, density={density}...")
    
    # Generate edges - each node connects to 'density' neighbors
    edges = []
    for i in range(num_nodes):
        src = i
        for offset in range(1, density + 1):
            dst = (i + offset) % num_nodes
            
            # Add label for 10% of nodes (deterministic seeds)
            if i % 10 == 0 and offset == 1:
                group_size = max(1, num_nodes // 4)
                label = (i // group_size) * 100  # 4 label groups
                edges.append(f"{src}\t{dst}\t{label}")
            else:
                edges.append(f"{src}\t{dst}")
    
    # Write local graph file
    print(f"Writing local file: {output_local}")
    with open(output_local, 'w') as f:
        f.write('\n'.join(edges))
    
    # Upload to S3 if requested
    if bucket and s3_prefix:
        print(f"Uploading to S3: {bucket}/{s3_prefix}/ ({num_partitions} partitions)")
        s3 = boto3.client(
            's3',
            endpoint_url=f"http://{endpoint}" if not endpoint.startswith("http") else endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Ensure bucket exists
        try:
            s3.create_bucket(Bucket=bucket)
        except:
            pass

        delete_existing_partitions(s3, bucket, s3_prefix)
        s3.put_object(
            Bucket=bucket,
            Key=s3_prefix,
            Body='\n'.join(edges).encode('utf-8'),
            ContentType='text/plain',
        )
        
        # Partition edges by source node modulo
        partitions = [[] for _ in range(num_partitions)]
        for edge in edges:
            src_node = int(edge.split('\t')[0])
            part_idx = src_node % num_partitions
            partitions[part_idx].append(edge)
        
        # Upload each partition
        for i, part_edges in enumerate(partitions):
            if part_edges:
                data = '\n'.join(part_edges).encode('utf-8')
                object_name = f"{s3_prefix}/part-{str(i).zfill(5)}"
                s3.put_object(
                    Bucket=bucket,
                    Key=object_name,
                    Body=data,
                    ContentType="text/plain"
                )
                print(f"  ✅ Partition {i}: {len(part_edges)} edges")
    
    print(f"✅ Graph generation complete: {num_nodes} nodes, {len(edges)} edges")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate large LP graph datasets")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--partitions", type=int, default=8, help="Number of S3 partitions")
    parser.add_argument("--output", type=str, default=None, help="Local output file")
    parser.add_argument("--bucket", type=str, default="test-bucket", help="S3 bucket")
    parser.add_argument("--prefix", type=str, default=None, help="S3 key prefix")
    parser.add_argument("--endpoint", type=str, default="localhost:9000", help="S3 endpoint")
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 upload")
    parser.add_argument("--density", type=int, default=10, help="Number of neighbors per node (graph density)")
    
    args = parser.parse_args()
    
    # Default file naming
    if args.output is None:
        args.output = f"large_{args.nodes}.txt"
    if args.prefix is None:
        args.prefix = f"graphs/large-{args.nodes}"
    
    generate_large_graph(
        num_nodes=args.nodes,
        num_partitions=args.partitions,
        output_local=args.output,
        bucket=None if args.no_s3 else args.bucket,
        s3_prefix=None if args.no_s3 else args.prefix,
        endpoint=args.endpoint,
        density=args.density
    )
