#!/usr/bin/env python3
"""Case 55: reader thread reads while another thread writes (append write and
truncate rewrite) to the same file. No crash/errors; final state sane."""
import os
import threading
import time
from common import run_case, rand_data

CHUNK = 4096
WRITES = 200
READS = 2000


def case(c, d):
    p = os.path.join(d, "shared")
    open(p, "wb").close()
    errors = []

    def writer():
        try:
            for i in range(WRITES):
                if i % 2 == 0:
                    with open(p, "ab") as f:          # append write
                        f.write(rand_data(CHUNK, seed=i))
                else:
                    with open(p, "wb") as f:          # truncate rewrite
                        f.write(rand_data(CHUNK, seed=1000 + i))
        except Exception as e:
            errors.append(e)

    def reader():
        try:
            for _ in range(READS):
                with open(p, "rb") as f:
                    f.read()
                time.sleep(0.0001)
        except Exception as e:
            errors.append(e)

    tw = threading.Thread(target=writer)
    tr = threading.Thread(target=reader)
    tr.start()
    tw.start()
    tw.join()
    tr.join()

    c.check_eq(errors, [], "no reader/writer thread errors")
    size = os.path.getsize(p)
    c.check(0 <= size <= WRITES * CHUNK, "final size sane: %d" % size)
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), size, "read length matches stat size")


if __name__ == "__main__":
    run_case("55_read_concurrent_write", case)
