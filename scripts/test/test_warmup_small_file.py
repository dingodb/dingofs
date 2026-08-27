#!/usr/bin/env python3
"""Concurrently create files in a directory."""

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


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

def parallel_create_files(directory: str, prefix: str, size: int, count: int, concurrency: int):
    success, failed = 0, 0
    start_all = time.monotonic()

    filenames = [f"{prefix}_{i:09d}.txt" for i in range(count)]

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(create_file, directory, name, size): name
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

    return filenames


def read_file_from_directory(directory: str):

    file_count = 0
    total_bytes = 0
    error_count = 0
    start = time.time()

    for path in iter_files(directory):
        file_start = time.perf_counter()
        try:
            with open(path, "rb") as f:
                data = f.read()
            file_elapsed = time.perf_counter() - file_start
            n = len(data)

            file_count += 1
            total_bytes += n

            print(f"{path}\t{n}\t{file_elapsed * 1000:.3f} ms")

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

def read_file_from_list(directory: str, filenames):

    file_count = 0
    total_bytes = 0
    error_count = 0
    start = time.time()

    for filename in filenames:
        path = os.path.join(directory, filename)
        file_start = time.perf_counter()
        try:
            with open(path, "rb") as f:
                data = f.read()
            file_elapsed = time.perf_counter() - file_start
            n = len(data)

            file_count += 1
            total_bytes += n

            print(f"{path}\t{n}\t{file_elapsed * 1000:.3f} ms")

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


def main():
    parser = argparse.ArgumentParser(description="Concurrently create files in a directory.")
    parser.add_argument("--directory", help="Target directory path")
    parser.add_argument("--concurrency", type=int, default=4,help="Number of concurrent workers")
    parser.add_argument("--prefix", default="file", help="File name prefix")
    parser.add_argument("--strategy", default="readdir", help="File reading strategy: readdir or list")
    parser.add_argument("--size", type=int, default=1048576, help="File size in bytes (default: 1048576)")
    parser.add_argument("-n", "--count", type=int, default=1000, help="Total number of files to create (default: 1000)")
    args = parser.parse_args()
    if args.size < 0:
        parser.error("--size must be non-negative")

    os.makedirs(args.directory, exist_ok=True)

    # create file
    filenames = parallel_create_files(args.directory, args.prefix, args.size, args.count, args.concurrency)

    # wait 30s
    print("Waiting for 30 seconds before reading files...")
    time.sleep(30)

    # read file
    print("Reading files...")
    if args.strategy == "readdir":
        read_file_from_directory(args.directory)
    elif args.strategy == "list":
        read_file_from_list(args.directory, filenames)
    else:
        parser.error("--strategy must be 'readdir' or 'list'")




if __name__ == "__main__":
    main()
