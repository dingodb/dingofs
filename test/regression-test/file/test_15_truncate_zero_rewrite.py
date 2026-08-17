#!/usr/bin/env python3
"""Case 15: truncate to 0 then rewrite."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "t")
    with open(p, "wb") as f:
        f.write(rand_data(500000, seed=15))
    os.truncate(p, 0)
    c.check_eq(os.path.getsize(p), 0, "size 0 after truncate")
    data2 = rand_data(300000, seed=155)
    with open(p, "r+b") as f:
        f.write(data2)
    with open(p, "rb") as f:
        c.check_eq(f.read(), data2, "rewritten content correct")


if __name__ == "__main__":
    run_case("15_truncate_zero_rewrite", case)
