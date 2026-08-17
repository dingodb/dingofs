#!/usr/bin/env python3
"""Case 23: unlink removes file; unlinked-but-open fd still readable/writable."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "u")
    data = rand_data(100000, seed=23)
    fd = os.open(p, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.write(fd, data)
        os.unlink(p)
        c.check(not os.path.exists(p), "path gone after unlink")
        c.check_eq(os.pread(fd, len(data), 0), data, "open fd still readable after unlink")
        extra = rand_data(1000, seed=233)
        os.pwrite(fd, extra, len(data))
        c.check_eq(os.pread(fd, 1000, len(data)), extra, "open fd still writable after unlink")
    finally:
        os.close(fd)
    c.check(not os.path.exists(p), "file stays deleted after close")


if __name__ == "__main__":
    run_case("23_unlink_open_fd", case)
