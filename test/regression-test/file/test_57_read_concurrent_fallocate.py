#!/usr/bin/env python3
"""Case 57: reader thread reads while another thread fallocates with different
modes (default alloc, keep-size, zero-range, punch-hole, collapse, insert).
Unsupported modes are skipped gracefully. No crash/errors."""
import os
import threading
import time
from common import (run_case, rand_data, fallocate, FALLOC_FL_KEEP_SIZE,
                    FALLOC_FL_PUNCH_HOLE, FALLOC_FL_COLLAPSE_RANGE,
                    FALLOC_FL_ZERO_RANGE, FALLOC_FL_INSERT_RANGE)

REGION = 1 << 20
READS = 2000
OPS = 300
MODES = [0, FALLOC_FL_KEEP_SIZE, FALLOC_FL_ZERO_RANGE, FALLOC_FL_PUNCH_HOLE,
         FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_INSERT_RANGE]


def case(c, d):
    p = os.path.join(d, "shared")
    with open(p, "wb") as f:
        f.write(rand_data(REGION, seed=57))
    errors = []
    unsupported = [0]

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

    def reader():
        try:
            for _ in range(READS):
                with open(p, "rb") as f:
                    f.read()
                time.sleep(0.0001)
        except Exception as e:
            errors.append(e)

    tf = threading.Thread(target=fallocator)
    tr = threading.Thread(target=reader)
    tr.start()
    tf.start()
    tf.join()
    tr.join()

    c.check_eq(errors, [], "no fallocator/reader thread errors")
    if unsupported[0]:
        print("  [info] %d fallocate ops unsupported, skipped" % unsupported[0])
    with open(p, "rb") as f:
        content = f.read()
    c.check_eq(len(content), os.path.getsize(p), "read length matches stat size")


if __name__ == "__main__":
    run_case("57_read_concurrent_fallocate", case)
