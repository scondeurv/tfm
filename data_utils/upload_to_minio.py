#!/usr/bin/env python3
"""Upload benchmark test data to MinIO/S3 (includes data generation)."""
import subprocess
import sys


if __name__ == "__main__":
    raise SystemExit(subprocess.call([sys.executable, "setup_large_lp_data.py", *sys.argv[1:]]))