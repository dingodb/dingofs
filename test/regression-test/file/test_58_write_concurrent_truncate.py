#!/usr/bin/env python3
"""Case 58: writer thread pwrites while another thread truncates to different
offsets. No crash/errors; final state sane."""
import os
import threading
from common import run_case, rand_data

MAX = 1 << 20
ROUNDS = 400


def case(c, d):
    p = os.path.join(d, "shared")
    with open(p, "wb") as f:
        f.truncate(MAX)
    errors = []

    def writer():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(ROUNDS):
                    off = (i * 4093) % (MAX - 4096)
                    os.pwrite(fd, rand_data(4096, seed=i), off)
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    def truncator():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(ROUNDS):
                    os.ftruncate(fd, (i * 131071) % (MAX + 1))
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    tw = threading.Thread(target=writer)
    tt = threading.Thread(target=truncator)
    tw.start()
    tt.start()
    tw.join()
    tt.join()

    c.check_eq(errors, [], "no writer/truncator thread errors")
    size = os.path.getsize(p)
    c.check(0 <= size <= MAX, "final size sane: %d" % size)
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), size, "read length matches stat size")


if __name__ == "__main__":
    run_case("58_write_concurrent_truncate", case)
