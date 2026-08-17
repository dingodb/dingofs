#!/usr/bin/env python3
"""Case 29: rename a directory containing files."""
import os
from common import run_case, rand_data


def case(c, d):
    src, dst = os.path.join(d, "src"), os.path.join(d, "dst")
    os.makedirs(os.path.join(src, "inner"))
    data = {}
    for i in range(10):
        data["f%d" % i] = rand_data(1000 + i, seed=i)
        with open(os.path.join(src, "f%d" % i), "wb") as f:
            f.write(data["f%d" % i])
    with open(os.path.join(src, "inner", "x"), "wb") as f:
        f.write(b"inner")
    os.rename(src, dst)
    c.check(not os.path.exists(src), "src dir gone")
    ok = all(open(os.path.join(dst, k), "rb").read() == v for k, v in data.items())
    c.check(ok, "all files intact after dir rename")
    with open(os.path.join(dst, "inner", "x"), "rb") as f:
        c.check_eq(f.read(), b"inner", "nested file intact")


if __name__ == "__main__":
    run_case("29_rename_directory", case)
