#!/usr/bin/env python3
"""Case 37: N processes concurrently write separate files, verify all."""
import multiprocessing as mp
import os
from common import run_case, rand_data, md5, md5_file


def worker(args):
    path, seed, size = args
    data = rand_data(size, seed=seed)
    with open(path, "wb") as f:
        f.write(data)
    return md5(data)


def case(c, d):
    n = int(os.environ.get("DINGOFS_CONC", "8"))
    size = 4 << 20
    jobs = [(os.path.join(d, "f%d" % i), i, size) for i in range(n)]
    with mp.Pool(n) as pool:
        digests = pool.map(worker, jobs)
    for (path, _, _), dig in zip(jobs, digests):
        c.check_eq(md5_file(path), dig, "md5 of %s" % os.path.basename(path))
        c.check_eq(os.path.getsize(path), size, "size of %s" % os.path.basename(path))


if __name__ == "__main__":
    run_case("37_concurrent_write_separate_files", case)
