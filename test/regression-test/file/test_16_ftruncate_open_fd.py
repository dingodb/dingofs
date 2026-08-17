#!/usr/bin/env python3
"""Case 16: ftruncate on an open fd, then continue read/write."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "t")
    data = rand_data(200000, seed=16)
    fd = os.open(p, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.write(fd, data)
        os.ftruncate(fd, 100000)
        c.check_eq(os.fstat(fd).st_size, 100000, "fstat size after ftruncate")
        c.check_eq(os.pread(fd, 100000, 0), data[:100000], "content after ftruncate")
        extra = rand_data(5000, seed=166)
        os.pwrite(fd, extra, 100000)
        c.check_eq(os.fstat(fd).st_size, 105000, "size after post-truncate write")
        c.check_eq(os.pread(fd, 5000, 100000), extra, "post-truncate written data")
    finally:
        os.close(fd)


if __name__ == "__main__":
    run_case("16_ftruncate_open_fd", case)
