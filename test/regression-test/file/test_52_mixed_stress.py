#!/usr/bin/env python3
"""Case 52 (slow): mixed stress - concurrent big-file streaming + many small files
+ metadata churn, all verified.
Env: DINGOFS_CONC (default 4), DINGOFS_STRESS_MB (big file MB per worker, default 128)."""
import multiprocessing as mp
import os
from common import run_case, rand_data, md5, md5_file


def big_writer(args):
    d, wid, size_mb = args
    p = os.path.join(d, "big%d" % wid)
    import hashlib
    h = hashlib.md5()
    with open(p, "wb") as f:
        for i in range(size_mb):
            buf = rand_data(1 << 20, seed=wid * 1000 + i)
            f.write(buf)
            h.update(buf)
    return (p, h.hexdigest())


def small_worker(args):
    d, wid = args
    errs = 0
    sub = os.path.join(d, "small%d" % wid)
    os.makedirs(sub, exist_ok=True)
    for i in range(300):
        p = os.path.join(sub, "f%d" % i)
        data = rand_data(1 + (i * 131) % 20000, seed=wid * 500 + i)
        with open(p, "wb") as f:
            f.write(data)
        if i % 3 == 0:
            os.rename(p, p + ".r")
            p += ".r"
        if i % 5 == 0:
            os.truncate(p, len(data) // 2)
            data = data[:len(data) // 2]
        with open(p, "rb") as f:
            if f.read() != data:
                errs += 1
        if i % 4 == 0:
            os.unlink(p)
    return errs


def case(c, d):
    n = int(os.environ.get("DINGOFS_CONC", "4"))
    size_mb = int(os.environ.get("DINGOFS_STRESS_MB", "128"))
    with mp.Pool(n * 2) as pool:
        big_r = pool.map_async(big_writer, [(d, i, size_mb) for i in range(n)])
        small_r = pool.map_async(small_worker, [(d, i) for i in range(n)])
        bigs = big_r.get()
        small_errs = small_r.get()
    c.check_eq(sum(small_errs), 0, "small-file workers had no mismatches")
    for p, dig in bigs:
        c.check_eq(md5_file(p), dig, "big file %s md5" % os.path.basename(p))


if __name__ == "__main__":
    run_case("52_mixed_stress", case)
