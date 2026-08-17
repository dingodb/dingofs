#!/usr/bin/env python3
"""Case 45: read beyond EOF returns partial/empty."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "x")
    data = rand_data(1000, seed=45)
    with open(p, "wb") as f:
        f.write(data)
    fd = os.open(p, os.O_RDONLY)
    try:
        c.check_eq(os.pread(fd, 100, 2000), b"", "pread past EOF returns empty")
        c.check_eq(os.pread(fd, 500, 800), data[800:], "pread crossing EOF returns partial (200B)")
    finally:
        os.close(fd)
    with open(p, "rb") as f:
        f.seek(5000)
        c.check_eq(f.read(), b"", "read after seek past EOF empty")


if __name__ == "__main__":
    run_case("45_read_beyond_eof", case)
