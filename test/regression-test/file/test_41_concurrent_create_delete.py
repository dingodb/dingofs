#!/usr/bin/env python3
"""Case 41: concurrent create/delete of many files in one directory."""
import multiprocessing as mp
import os
from common import run_case

PER_PROC = 200


def worker(args):
    d, wid = args
    errs = 0
    for i in range(PER_PROC):
        p = os.path.join(d, "w%d_f%d" % (wid, i))
        try:
            with open(p, "wb") as f:
                f.write(b"x" * 100)
            if os.path.getsize(p) != 100:
                errs += 1
            if i % 2 == 0:
                os.unlink(p)
        except OSError:
            errs += 1
    return errs


def case(c, d):
    n = int(os.environ.get("DINGOFS_CONC", "8"))
    with mp.Pool(n) as pool:
        errs = pool.map(worker, [(d, i) for i in range(n)])
    c.check_eq(sum(errs), 0, "no errors during concurrent create/delete")
    remaining = os.listdir(d)
    c.check_eq(len(remaining), n * PER_PROC // 2, "remaining file count (odd-indexed kept)")
    c.check(all(os.path.getsize(os.path.join(d, f)) == 100 for f in remaining),
            "remaining files intact")


if __name__ == "__main__":
    run_case("41_concurrent_create_delete", case)
