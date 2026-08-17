#!/usr/bin/env python3
"""Case 61: multi-thread concurrent readers, writers (append write + truncate
rewrite) and truncators (different offsets) on one file. No crash/errors.

Env: DINGOFS_CONC (default 8)."""
import os
import threading
import time
from common import run_case, rand_data

CHUNK = 4096
MAX = 1 << 20
ITER = 100


def case(c, d):
    p = os.path.join(d, "shared")
    with open(p, "wb") as f:
        f.write(rand_data(MAX, seed=61))
    n = int(os.environ.get("DINGOFS_CONC", "8"))
    errors = []
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

    nr = max(1, n // 2)
    nw = max(1, n // 4)
    nt = max(1, n // 4)
    readers = [threading.Thread(target=reader) for _ in range(nr)]
    writers = [threading.Thread(target=writer) for _ in range(nw)]
    truncators = [threading.Thread(target=truncator) for _ in range(nt)]
    for t in readers + writers + truncators:
        t.start()
    for t in writers + truncators:
        t.join()
    stop.set()
    for t in readers:
        t.join()

    c.check_eq(errors, [], "no thread errors")
    size = os.path.getsize(p)
    # truncators extend at most to MAX; appends add CHUNK each (ITER//2 per writer)
    bound = MAX + nw * (ITER // 2) * CHUNK
    c.check(0 <= size <= bound, "final size sane: %d" % size)
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), size, "read length matches stat size")


if __name__ == "__main__":
    run_case("61_mixed_concurrent_rw_truncate", case)
