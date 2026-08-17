#!/usr/bin/env python3
"""Case 39: concurrent O_APPEND from multiple processes; total size correct,
records not torn (each record is one write of fixed size tagged by writer id)."""
import multiprocessing as mp
import os
from collections import Counter
from common import run_case

REC = 4096
NREC = 100


def worker(args):
    path, wid = args
    fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    try:
        for _ in range(NREC):
            os.write(fd, bytes([wid]) * REC)
    finally:
        os.close(fd)


def case(c, d):
    p = os.path.join(d, "app")
    open(p, "wb").close()
    n = int(os.environ.get("DINGOFS_CONC", "6"))
    with mp.Pool(n) as pool:
        pool.map(worker, [(p, i + 1) for i in range(n)])
    size = os.path.getsize(p)
    c.check_eq(size, n * NREC * REC, "total size = sum of appends")
    torn = 0
    counts = Counter()
    with open(p, "rb") as f:
        for _ in range(size // REC):
            rec = f.read(REC)
            if len(set(rec)) != 1:
                torn += 1
            else:
                counts[rec[0]] += 1
    c.check_eq(torn, 0, "no torn records")
    c.check(all(counts[i + 1] == NREC for i in range(n)),
            "each writer's %d records present: %s" % (NREC, dict(counts)))


if __name__ == "__main__":
    run_case("39_concurrent_append", case)
