#!/usr/bin/env python3
"""Concurrently create files in a directory."""

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def create_file(directory: str, filename: str, size: int) -> tuple[str, float]:
    filepath = os.path.join(directory, filename)
    start = time.monotonic()
    with open(filepath, "wb") as f:
        remaining = size
        while remaining:
            chunk_size = min(remaining, 1024 * 1024)
            f.write(os.urandom(chunk_size))
            remaining -= chunk_size
    elapsed = time.monotonic() - start
    return filepath, elapsed


def main():
    parser = argparse.ArgumentParser(description="Concurrently create files in a directory.")
    parser.add_argument("--directory", help="Target directory path")
    parser.add_argument("--concurrency", type=int, help="Number of concurrent workers")
    parser.add_argument("--prefix", help="File name prefix")
    parser.add_argument("--size", type=int, default=0, help="File size in bytes (default: 0)")
    parser.add_argument("-n", "--count", type=int, default=10000, help="Total number of files to create (default: 10000)")
    args = parser.parse_args()
    if args.size < 0:
        parser.error("--size must be non-negative")

    os.makedirs(args.directory, exist_ok=True)

    filenames = [f"{args.prefix}_{i:09d}.txt" for i in range(args.count)]

    success, failed = 0, 0
    start_all = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(create_file, args.directory, name, args.size): name
            for name in filenames
        }
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
