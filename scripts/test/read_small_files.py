#!/usr/bin/env python3
"""Read all files under a directory (recursively) and report stats.

Usage:
    python3 read_small_files.py <directory> [--quiet]
"""

import argparse
import os
import sys
import time


def iter_files(root):
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            yield os.path.join(dirpath, name)


def human_bytes(n):
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024


def main():
    parser = argparse.ArgumentParser(description="Read all files under a directory.")
    parser.add_argument("--directory", help="Directory path to scan")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output")
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"error: not a directory: {args.directory}", file=sys.stderr)
        return 2

    file_count = 0
    total_bytes = 0
    error_count = 0
    start = time.time()

    for path in iter_files(args.directory):
        try:
            with open(path, "rb") as f:
                data = f.read()
            n = len(data)
            file_count += 1
            total_bytes += n
            if not args.quiet:
                print(f"{path}\t{n}")
        except OSError as e:
            error_count += 1
            print(f"error reading {path}: {e}", file=sys.stderr)

    elapsed = time.time() - start
    throughput = total_bytes / elapsed if elapsed > 0 else 0
    print("---")
    print(f"files read : {file_count}")
    print(f"errors     : {error_count}")
    print(f"total size : {human_bytes(total_bytes)} ({total_bytes} bytes)")
    print(f"elapsed    : {elapsed:.3f} s")
    print(f"throughput : {human_bytes(throughput)}/s")
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
