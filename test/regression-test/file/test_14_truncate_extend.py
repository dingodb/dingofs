#!/usr/bin/env python3
"""Case 14: truncate to larger size, extended region reads zeros."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "t")
    data = rand_data(10000, seed=14)
    with open(p, "wb") as f:
        f.write(data)
    new_size = 1 << 20
    os.truncate(p, new_size)
    c.check_eq(os.path.getsize(p), new_size, "size after extend")
    with open(p, "rb") as f:
        got = f.read()
    c.check_eq(got[:10000], data, "original data intact")
    c.check(got[10000:] == b"\x00" * (new_size - 10000), "extended region is zeros")


if __name__ == "__main__":
    run_case("14_truncate_extend", case)
