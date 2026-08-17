#!/usr/bin/env python3
"""Case 60: writer thread pwrites while another thread fallocates with different
modes. Unsupported modes skipped. No crash/errors."""
import os
import threading
from common import (run_case, rand_data, fallocate, FALLOC_FL_KEEP_SIZE,
                    FALLOC_FL_PUNCH_HOLE, FALLOC_FL_COLLAPSE_RANGE,
                    FALLOC_FL_ZERO_RANGE, FALLOC_FL_INSERT_RANGE)

REGION = 1 << 20
OPS = 300
MODES = [0, FALLOC_FL_KEEP_SIZE, FALLOC_FL_ZERO_RANGE, FALLOC_FL_PUNCH_HOLE,
         FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_INSERT_RANGE]


def case(c, d):
    p = os.path.join(d, "shared")
    with open(p, "wb") as f:
        f.truncate(REGION)
    errors = []
    unsupported = [0]

    def writer():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(OPS):
                    off = (i * 4093) % (REGION - 4096)
                    os.pwrite(fd, rand_data(4096, seed=i), off)
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    def fallocator():
        try:
            fd = os.open(p, os.O_RDWR)
            try:
                for i in range(OPS):
                    mode = MODES[i % len(MODES)]
                    off = (i * 4093) % (REGION - 4096)
                    if not fallocate(fd, mode, off, 4096):
                        unsupported[0] += 1
            finally:
                os.close(fd)
        except Exception as e:
            errors.append(e)

    tw = threading.Thread(target=writer)
    tf = threading.Thread(target=fallocator)
    tw.start()
    tf.start()
    tw.join()
    tf.join()

    c.check_eq(errors, [], "no writer/fallocator thread errors")
    if unsupported[0]:
        print("  [info] %d fallocate ops unsupported, skipped" % unsupported[0])
    with open(p, "rb") as f:
        c.check_eq(len(f.read()), os.path.getsize(p), "read length matches stat size")


if __name__ == "__main__":
    run_case("60_write_concurrent_fallocate", case)
