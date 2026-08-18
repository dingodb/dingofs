#!/usr/bin/env python3
"""Run all DingoFS file-operation test cases.

Usage:
    python3 run_all.py [TEST_DIR] [--skip-slow] [--only 01,05,37] [--rounds N]
Env:
    DINGOFS_TEST_DIR   test root (if no positional arg)
"""
import glob
import os
import subprocess
import sys
import time

SLOW = {"05", "27", "51", "52", "53"}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    args = sys.argv[1:]
    skip_slow = "--skip-slow" in args
    only = None
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))
    rounds = 1
    if "--rounds" in args:
        rounds = max(1, int(args[args.index("--rounds") + 1]))
    pos = [a for a in args if not a.startswith("--")]
    if only:
        pos = [a for a in pos if a != args[args.index("--only") + 1]]
    if "--rounds" in args:
        pos = [a for a in pos if a != args[args.index("--rounds") + 1]]
    test_dir = pos[0] if pos else None

    scripts = sorted(glob.glob(os.path.join(here, "test_*.py")))
    results = []
    for rnd in range(1, rounds + 1):
        for s in scripts:
            num = os.path.basename(s).split("_")[1]
            if skip_slow and num in SLOW:
                results.append((rnd, s, "SKIP", 0))
                continue
            if only and num not in only:
                continue
            cmd = [sys.executable, s] + ([test_dir] if test_dir else [])
            t0 = time.time()
            rc = subprocess.call(cmd, cwd=here)
            results.append(
                (rnd, s, "PASS" if rc == 0 else "FAIL", time.time() - t0))

    print("\n" + "=" * 70)
    print("%-55s %-6s %8s" % ("SCRIPT", "RESULT", "TIME(s)"))
    print("-" * 70)
    fails = 0
    skips = 0
    for rnd, s, r, t in results:
        name = os.path.basename(s)
        if rounds > 1:
            name = "%s (round %d)" % (name, rnd)
        print("%-55s %-6s %8.1f" % (name, r, t))
        fails += r == "FAIL"
        skips += r == "SKIP"
    print("=" * 70)
    print("TOTAL: %d, FAIL: %d, SKIP: %d" % (len(results), fails, skips))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
