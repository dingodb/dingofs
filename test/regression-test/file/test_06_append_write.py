#!/usr/bin/env python3
"""Case 06: multiple appends, content and order correct."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "app")
    parts = [rand_data(1000 + i * 37, seed=i) for i in range(20)]
    for part in parts:
        with open(p, "ab") as f:
            f.write(part)
    expect = b"".join(parts)
    with open(p, "rb") as f:
        c.check_eq(f.read(), expect, "appended content matches")
    c.check_eq(os.path.getsize(p), len(expect), "appended size")


if __name__ == "__main__":
    run_case("06_append_write", case)
