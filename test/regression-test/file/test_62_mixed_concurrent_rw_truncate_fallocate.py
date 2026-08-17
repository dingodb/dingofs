#!/usr/bin/env python3
"""Case 62: multi-thread concurrent readers, writers (append write + truncate
rewrite), truncators (different offsets) and fallocators (different modes) on
one file. No crash/errors.

Env: DINGOFS_CONC (default 8)."""
import os
import threading
import time
from common import (run_case, rand_data, fallocate, FALLOC_FL_KEEP_SIZE,
                    FALLOC_FL_PUNCH_HOLE, FALLOC_FL_COLLAPSE_RANGE,
                    FALLOC_FL_ZERO_RANGE, FALLOC_FL_INSERT_RANGE)

CHUNK = 4096
MAX = 1 << 20
ITER = 100
MODES = [0, FALLOC_FL_KEEP_SIZE, FALLOC_FL_ZERO_RANGE, FALLOC_FL_PUNCH_HOLE,
         FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_INSERT_RANGE]


def case(c, d):
    p = os.path.join(d, "shared")
    with open(p, "wb") as f:
        f.write(rand_data(MAX, seed=62))
    n = int(os.environ.get("DINGOFS_CONC", "8"))
    errors = []
    unsupported = [0]
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                with open(p, "rb") as f:
                    f.read()
                time.sleep(0.0001)
        except Exception as e:
            errors.append(e)

    def writer():
        try:
            for i in range(ITER):
                if i % 2 == 0:
                    with open(p, "ab") as f:          # append write
                        f.write(rand_data(CHUNK, seed=i))
                else:
                    with open(p, "wb") as f:          # truncate rewrite
                        f.write(rand_data(CHUNK, seed=1000 + i))
        except Exception as e:
            errors.append(e)

    def truncator():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(ITER):
                    os.ftruncate(fd, (i * 131071) % (MAX + 1))
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    def fallocator():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(ITER):
                    mode = MODES[i % len(MODES)]
                    off = (i * 4093) % (MAX - 4096)
                    if not fallocate(fd, mode, off, 4096):
                        unsupported[0] += 1
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    nr = max(1, n // 2)
    nw = max(1, n // 4)
    nt = max(1, n // 4)
    nf = max(1, n // 4)
    readers = [threading.Thread(target=reader) for _ in range(nr)]
    writers = [threading.Thread(target=writer) for _ in range(nw)]
    truncators = [threading.Thread(target=truncator) for _ in range(nt)]
    fallocators = [threading.Thread(target=fallocator) for _ in range(nf)]
    for t in readers + writers + truncators + fallocators:
        t.start()
    for t in writers + truncators + fallocators:
        t.join()
    stop.set()
    for t in readers:
        t.join()

    c.check_eq(errors, [], "no thread errors")
    if unsupported[0]:
        print("  [info] %d fallocate ops unsupported, skipped" % unsupported[0])
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), os.path.getsize(p), "read length matches stat size")


if __name__ == "__main__":
    run_case("62_mixed_concurrent_rw_truncate_fallocate", case)
