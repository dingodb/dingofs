#!/usr/bin/env python3
"""Case 44: concurrent truncate + write mix; no crashes, final state self-consistent."""
import multiprocessing as mp
import os
import random
from common import run_case

MAX_SIZE = 4 << 20


def truncator(args):
    path, seed = args
    rnd = random.Random(seed)
    errs = 0
    for _ in range(100):
        try:
            os.truncate(path, rnd.randrange(0, MAX_SIZE))
        except OSError:
            errs += 1
    return errs


def writer_proc(args):
    path, seed = args
    rnd = random.Random(seed)
    errs = 0
    fd = os.open(path, os.O_WRONLY)
    try:
        for _ in range(100):
            off = rnd.randrange(0, MAX_SIZE)
            try:
                os.pwrite(fd, b"W" * 4096, off)
            except OSError:
                errs += 1
    finally:
        os.close(fd)
    return errs


def case(c, d):
    p = os.path.join(d, "mix")
    with open(p, "wb") as f:
        f.truncate(MAX_SIZE)
    with mp.Pool(6) as pool:
        r1 = pool.map_async(truncator, [(p, i) for i in range(3)])
        r2 = pool.map_async(writer_proc, [(p, 100 + i) for i in range(3)])
        errs = sum(r1.get()) + sum(r2.get())
    c.check_eq(errs, 0, "no OS errors during truncate/write race")
    size = os.path.getsize(p)
    c.check(0 <= size <= MAX_SIZE + 4096, "final size sane: %d" % size)
    with open(p, "rb") as f:
        got = f.read()
    c.check_eq(len(got), size, "read length matches stat size")
    c.check(set(got) <= {0, ord("W")}, "content only zeros or written bytes")


if __name__ == "__main__":
    run_case("44_truncate_write_race", case)
