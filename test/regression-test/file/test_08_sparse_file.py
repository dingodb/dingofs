#!/usr/bin/env python3
"""Case 08: sparse file - write past EOF, hole reads as zeros, size correct."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "sparse")
    head = rand_data(1000, seed=8)
    tail = rand_data(2000, seed=88)
    hole_off = 10 << 20
    with open(p, "wb") as f:
        f.write(head)
        f.seek(hole_off)
        f.write(tail)
    c.check_eq(os.path.getsize(p), hole_off + len(tail), "sparse file size")
    with open(p, "rb") as f:
        c.check_eq(f.read(1000), head, "head data")
        f.seek(len(head))
        hole = f.read(hole_off - len(head))
        c.check(hole == b"\x00" * len(hole), "hole reads as zeros")
        f.seek(hole_off)
        c.check_eq(f.read(), tail, "tail data")


if __name__ == "__main__":
    run_case("08_sparse_file", case)
