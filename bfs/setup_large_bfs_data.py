#!/usr/bin/env python3
from pathlib import Path
import runpy


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    runpy.run_path(str(root / "data_utils" / "setup_large_bfs_data.py"), run_name="__main__")


if __name__ == "__main__":
    main()
