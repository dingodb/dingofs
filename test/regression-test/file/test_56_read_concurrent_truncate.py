#!/usr/bin/env python3
"""Case 56: reader thread reads while another thread truncates the file to
different offsets. No crash/errors; final state sane."""
import os
import threading
import time
from common import run_case, rand_data

MAX = 1 << 20
READS = 2000
TRUNCS = 400


def case(c, d):
    p = os.path.join(d, "shared")
    with open(p, "wb") as f:
        f.write(rand_data(MAX, seed=56))
    errors = []

    def truncator():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(TRUNCS):
                    os.ftruncate(fd, (i * 131071) % (MAX + 1))
            finally:
                os.close(fd)
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

    tt = threading.Thread(target=truncator)
    tr = threading.Thread(target=reader)
    tr.start()
    tt.start()
    tt.join()
    tr.join()

    c.check_eq(errors, [], "no truncator/reader thread errors")
    size = os.path.getsize(p)
    c.check(0 <= size <= MAX, "final size sane: %d" % size)
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), size, "read length matches stat size")


if __name__ == "__main__":
    run_case("56_read_concurrent_truncate", case)
