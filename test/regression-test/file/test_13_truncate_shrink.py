#!/usr/bin/env python3
"""Case 13: truncate to smaller size."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "t")
    data = rand_data(1 << 20, seed=13)
    with open(p, "wb") as f:
        f.write(data)
    os.truncate(p, 12345)
    c.check_eq(os.path.getsize(p), 12345, "size after shrink")
    with open(p, "rb") as f:
        c.check_eq(f.read(), data[:12345], "content truncated correctly")


if __name__ == "__main__":
    run_case("13_truncate_shrink", case)
