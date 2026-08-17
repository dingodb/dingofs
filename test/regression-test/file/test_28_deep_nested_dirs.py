#!/usr/bin/env python3
"""Case 28: deep nested directory create and traverse."""
import os
from common import run_case


def case(c, d):
    depth = 50
    path = d
    for i in range(depth):
        path = os.path.join(path, "d%02d" % i)
    os.makedirs(path)
    c.check(os.path.isdir(path), "deep path created (depth=%d)" % depth)
    fp = os.path.join(path, "leaf")
    with open(fp, "wb") as f:
        f.write(b"leafdata")
    with open(fp, "rb") as f:
        c.check_eq(f.read(), b"leafdata", "leaf file in deep dir")
    count = sum(len(dirs) for _, dirs, _ in os.walk(d))
    c.check_eq(count, depth, "os.walk finds all %d dirs" % depth)


if __name__ == "__main__":
    run_case("28_deep_nested_dirs", case)
