#!/usr/bin/env python3
"""Concurrently create files in a directory."""

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def create_file(directory: str, filename: str) -> tuple[str, float]:
    filepath = os.path.join(directory, filename)
    start = time.monotonic()
    with open(filepath, "w") as f:
        # f.write(f"created: {filename}\n")
        pass
    elapsed = time.monotonic() - start
    return filepath, elapsed


def main():
    parser = argparse.ArgumentParser(description="Concurrently create files in a directory.")
    parser.add_argument("--directory", help="Target directory path")
    parser.add_argument("--concurrency", type=int, help="Number of concurrent workers")
    parser.add_argument("--prefix", help="File name prefix")
    parser.add_argument("-n", "--count", type=int, default=10000, help="Total number of files to create (default: 10000)")
    args = parser.parse_args()

    os.makedirs(args.directory, exist_ok=True)

    filenames = [f"{args.prefix}_{i:09d}.txt" for i in range(args.count)]

    success, failed = 0, 0
    start_all = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {executor.submit(create_file, args.directory, name): name for name in filenames}
        for future in as_completed(futures):
            try:
                filepath, elapsed = future.result()
                success += 1
                print(f"[OK] {filepath} ({elapsed*1000:.1f}ms)")
            except Exception as e:
                failed += 1
                print(f"[FAIL] {futures[future]}: {e}")

    total = time.monotonic() - start_all
    print(f"\nDone: {success} created, {failed} failed in {total:.2f}s "
          f"({success/total:.1f} files/s)")


if __name__ == "__main__":
    main()
