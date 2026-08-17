#!/usr/bin/env python3
"""Case 22: rename over an existing target file."""
import os
from common import run_case, rand_data


def case(c, d):
    src, dst = os.path.join(d, "a"), os.path.join(d, "b")
    data_src = rand_data(1000, seed=22)
    with open(src, "wb") as f:
        f.write(data_src)
    with open(dst, "wb") as f:
        f.write(rand_data(2000, seed=222))
    os.rename(src, dst)
    c.check(not os.path.exists(src), "src gone")
    with open(dst, "rb") as f:
        c.check_eq(f.read(), data_src, "target replaced with src content")


if __name__ == "__main__":
    run_case("22_rename_overwrite", case)
