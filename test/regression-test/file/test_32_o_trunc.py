#!/usr/bin/env python3
"""Case 32: open with O_TRUNC empties the file."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "x")
    with open(p, "wb") as f:
        f.write(rand_data(100000, seed=32))
    fd = os.open(p, os.O_WRONLY | os.O_TRUNC)
    try:
        c.check_eq(os.fstat(fd).st_size, 0, "size 0 after O_TRUNC open")
        os.write(fd, b"new")
    finally:
        os.close(fd)
    with open(p, "rb") as f:
        c.check_eq(f.read(), b"new", "only new data present")


if __name__ == "__main__":
    run_case("32_o_trunc", case)
