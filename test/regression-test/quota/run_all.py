#!/usr/bin/env python3
"""Run all DingoFS quota regression cases.

Usage:
    python3 run_all.py [TEST_DIR] [--only 01,04] [--skip-slow]

Environment:
    DINGOFS_MDS_ADDR, DINGOFS_FS_ID, DINGOFS_ROOT_INO,
    DINGOFS_TEST_DIR, DINGOFS_QUOTA_WAIT
"""
import glob
import os
import subprocess
import sys
import time


SLOW = {"04", "05"}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    args = sys.argv[1:]
    skip_slow = "--skip-slow" in args
    only = None
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))
    positional = [a for a in args if not a.startswith("--")]
    if only:
        positional = [a for a in positional if a != args[args.index("--only") + 1]]
    test_dir = positional[0] if positional else None

    scripts = sorted(glob.glob(os.path.join(here, "test_*.py")))
    results = []
    for script in scripts:
        number = os.path.basename(script).split("_")[1]
        if only and number not in only:
            continue
        if skip_slow and number in SLOW:
            results.append((script, "SKIP", 0))
            continue
        command = [sys.executable, script] + ([test_dir] if test_dir else [])
        started = time.time()
        rc = subprocess.call(command, cwd=here)
        results.append((script, "PASS" if rc == 0 else "FAIL", time.time() - started))

    print("\n" + "=" * 70)
    print("%-55s %-6s %8s" % ("SCRIPT", "RESULT", "TIME(s)"))
    print("-" * 70)
    failures = skips = 0
    for script, result, elapsed in results:
        print("%-55s %-6s %8.1f" % (os.path.basename(script), result, elapsed))
        failures += result == "FAIL"
        skips += result == "SKIP"
    print("=" * 70)
    print("TOTAL: %d, FAIL: %d, SKIP: %d" % (len(results), failures, skips))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
