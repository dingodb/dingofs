#!/usr/bin/env python3
"""Summarize FUSE request latencies for a directory's direct children."""

import argparse
import math
import re
from collections import defaultdict

OPERATIONS = ("lookup", "open", "read", "flush", "release")
LOOKUP_RE = re.compile(r"\blookup \((\d+)/[^)]*\):.*?<([0-9.]+)>")
LOOKUP_INODE_RE = re.compile(r":\s*OK\s+\((\d+),\[")
OPERATION_RE = re.compile(r"\b(open|read|flush|release) \((\d+)(?:,|\)).*?<([0-9.]+)>")


def percentile(values, percent):
    """Return the nearest-rank percentile from sorted values."""
    return values[math.ceil(percent * len(values)) - 1]


def collect(lines, directory_ino):
    """Collect seconds of requests associated with direct children."""
    latencies = defaultdict(list)
    child_inos = set()

    for line in lines:
        lookup = LOOKUP_RE.search(line)
        if lookup and lookup.group(1) == directory_ino:
            latencies["lookup"].append(float(lookup.group(2)))
            result = LOOKUP_INODE_RE.search(line)
            if result:
                child_inos.add(result.group(1))
            continue

        operation = OPERATION_RE.search(line)
        if operation and operation.group(2) in child_inos:
            latencies[operation.group(1)].append(float(operation.group(3)))

    return latencies


def print_summary(latencies):
    print(f"{'operation':<10} {'count':>7} {'avg(us)':>12} {'p55(us)':>12} "
          f"{'p90(us)':>12} {'p95(us)':>12} {'p99(us)':>12} {'max(us)':>12}")
    for operation in OPERATIONS:
        values = sorted(value * 1_000_000 for value in latencies[operation])
        if not values:
            print(f"{operation:<10} {0:>7} {'-':>12} {'-':>12} {'-':>12} "
                  f"{'-':>12} {'-':>12} {'-':>12}")
            continue
        print(f"{operation:<10} {len(values):>7} {sum(values) / len(values):>12.3f} "
              f"{percentile(values, .55):>12.3f} {percentile(values, .90):>12.3f} "
              f"{percentile(values, .95):>12.3f} {percentile(values, .99):>12.3f} "
              f"{values[-1]:>12.3f}")


def self_test():
    lines = [
        "lookup (42/file): OK  (7,[-rw-r--r--]) <0.000010>\n",
        "open (7): RDONLY OK <0.000020>\n",
        "read (7,4096,0): OK <0.000030>\n",
        "flush (7): error <0.000040>\n",
        "release (7): OK <0.000050>\n",
        "open (8): OK <0.000060>\n",
    ]
    latencies = collect(lines, "42")
    assert [value * 1_000_000 for value in latencies["lookup"]] == [10.0]
    assert percentile(sorted(latencies["read"]), .55) == 0.00003
    assert "open" in latencies and len(latencies["open"]) == 1
    assert "release" in latencies and len(latencies["release"]) == 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_file", nargs="?")
    parser.add_argument("directory_ino", nargs="?")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.log_file or not args.directory_ino:
        parser.error("log_file and directory_ino are required")

    with open(args.log_file, encoding="utf-8", errors="replace") as log_file:
        print_summary(collect(log_file, args.directory_ino))


if __name__ == "__main__":
    main()
