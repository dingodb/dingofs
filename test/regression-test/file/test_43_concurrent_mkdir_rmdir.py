#!/usr/bin/env python3
"""Case 43: concurrent mkdir/rmdir on the same path - no crashes, sane errnos."""
import errno
import multiprocessing as mp
import os
import random
from common import run_case

ALLOWED = {errno.EEXIST, errno.ENOENT, errno.ENOTEMPTY, errno.EBUSY}


def worker(args):
    path, seed = args
    rnd = random.Random(seed)
    bad = 0
    for _ in range(100):
        try:
            if rnd.random() < 0.5:
                os.mkdir(path)
            else:
                os.rmdir(path)
        except OSError as e:
            if e.errno not in ALLOWED:
                bad += 1
    return bad


def case(c, d):
    target = os.path.join(d, "contended")
    n = 8
    with mp.Pool(n) as pool:
        bads = pool.map(worker, [(target, i) for i in range(n)])
    c.check_eq(sum(bads), 0, "only expected errnos during mkdir/rmdir race")
    exists = os.path.exists(target)
    c.check(not exists or os.path.isdir(target), "final state sane (absent or a dir)")


if __name__ == "__main__":
    run_case("43_concurrent_mkdir_rmdir", case)
