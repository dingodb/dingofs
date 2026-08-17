#!/usr/bin/env python3
"""Case 54: open() with each mode character - r, a, w, x, t, b - verified."""
import errno
import os
from common import run_case, rand_data


def case(c, d):
    # 'r' - read only, existing content
    pr = os.path.join(d, "r.txt")
    with open(pr, "wb") as f:
        f.write(b"hello")
    with open(pr, "r") as f:
        c.check_eq(f.read(), "hello", "'r' reads existing content")

    # 'a' - append writes always go to end, ignoring seek
    pa = os.path.join(d, "a.txt")
    open(pa, "wb").close()
    with open(pa, "a") as f:
        f.write("first")
        f.seek(0)
        f.write("second")
    with open(pa, "rb") as f:
        c.check_eq(f.read(), b"firstsecond", "'a' appends despite seek")

    # 'w' - truncates existing content then writes
    pw = os.path.join(d, "w.txt")
    with open(pw, "wb") as f:
        f.write(b"old")
    with open(pw, "w") as f:
        f.write("new")
    with open(pw, "rb") as f:
        c.check_eq(f.read(), b"new", "'w' truncates then writes")

    # 'x' - exclusive create; fails EEXIST when file exists
    px = os.path.join(d, "x.txt")
    with open(px, "x") as f:
        f.write("only")
    c.check_raises([errno.EEXIST], lambda: open(px, "x"),
                   "'x' on existing file raises EEXIST")

    # 't' - text mode (str objects round-trip)
    pt = os.path.join(d, "t.txt")
    with open(pt, "wt") as f:
        f.write("line1\nline2\n")
    with open(pt, "rt") as f:
        c.check_eq(f.read(), "line1\nline2\n", "'t' text mode round-trips")

    # 'b' - binary mode (bytes objects round-trip)
    data = rand_data(100000, seed=54)
    pb = os.path.join(d, "b.bin")
    with open(pb, "wb") as f:
        f.write(data)
    with open(pb, "rb") as f:
        c.check_eq(f.read(), data, "'b' binary mode round-trips bytes")


if __name__ == "__main__":
    run_case("54_open_modes", case)
