#!/usr/bin/env python3
"""Case 59: writer thread overwrites while another thread writes data (append
write and truncate rewrite). No crash/errors; final state sane."""
import os
import threading
from common import run_case, rand_data

CHUNK = 4096
ROUNDS = 200


def case(c, d):
    p = os.path.join(d, "shared")
    open(p, "wb").close()
    errors = []

    def overwriter():
        try:
            fd = os.open(p, os.O_WRONLY | os.O_CREAT, 0o644)
            try:
                for i in range(ROUNDS):
                    os.pwrite(fd, rand_data(CHUNK, seed=10000 + i), 0)
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    def appender_truncater():
        try:
            for i in range(ROUNDS):
                if i % 2 == 0:
                    with open(p, "ab") as f:          # append write
                        f.write(rand_data(CHUNK, seed=i))
                else:
                    with open(p, "wb") as f:          # truncate rewrite
                        f.write(rand_data(CHUNK, seed=20000 + i))
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=overwriter)
    t2 = threading.Thread(target=appender_truncater)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    c.check_eq(errors, [], "no writer thread errors")
    size = os.path.getsize(p)
    c.check(0 <= size <= ROUNDS * CHUNK, "final size sane: %d" % size)
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), size, "read length matches stat size")


if __name__ == "__main__":
    run_case("59_write_concurrent_write", case)
