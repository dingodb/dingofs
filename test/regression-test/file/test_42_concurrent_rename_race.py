#!/usr/bin/env python3
"""Case 42: concurrent rename race - multiple processes rename same source;
exactly one succeeds (others get ENOENT)."""
import errno
import multiprocessing as mp
import os
from common import run_case


def worker(args):
    src, dst = args
    try:
        os.rename(src, dst)
        return "ok"
    except OSError as e:
        return "errno%d" % e.errno


def case(c, d):
    src = os.path.join(d, "src")
    with open(src, "wb") as f:
        f.write(b"race")
    n = 8
    jobs = [(src, os.path.join(d, "dst%d" % i)) for i in range(n)]
    with mp.Pool(n) as pool:
        results = pool.map(worker, jobs)
    wins = results.count("ok")
    c.check_eq(wins, 1, "exactly one rename succeeded (results=%s)" % results)
    losers_ok = all(r == "ok" or r == "errno%d" % errno.ENOENT for r in results)
    c.check(losers_ok, "losers got ENOENT")
    c.check(not os.path.exists(src), "src gone")
    dsts = [j[1] for j in jobs if os.path.exists(j[1])]
    c.check_eq(len(dsts), 1, "exactly one destination exists")
    if dsts:
        with open(dsts[0], "rb") as f:
            c.check_eq(f.read(), b"race", "content intact")


if __name__ == "__main__":
    run_case("42_concurrent_rename_race", case)
