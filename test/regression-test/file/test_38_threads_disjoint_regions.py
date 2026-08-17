#!/usr/bin/env python3
"""Case 38: multiple threads pwrite disjoint regions of one file, verify whole."""
import os
import threading
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "shared")
    nthreads = int(os.environ.get("DINGOFS_CONC", "8"))
    region = 2 << 20
    with open(p, "wb") as f:
        f.truncate(nthreads * region)
    fd = os.open(p, os.O_WRONLY)
    datas = [rand_data(region, seed=i) for i in range(nthreads)]
    errors = []

    def w(i):
        try:
            off = i * region
            for j in range(0, region, 256 * 1024):
                os.pwrite(fd, datas[i][j:j + 256 * 1024], off + j)
        except Exception as e:
            errors.append(e)

    ts = [threading.Thread(target=w, args=(i,)) for i in range(nthreads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    os.close(fd)
    c.check_eq(errors, [], "no thread errors")
    with open(p, "rb") as f:
        for i in range(nthreads):
            f.seek(i * region)
            c.check_eq(f.read(region) == datas[i], True, "region %d correct" % i)


if __name__ == "__main__":
    run_case("38_threads_disjoint_regions", case)
