#!/usr/bin/env python3
"""Case 20: utime sets timestamps, stat agrees."""
import os
from common import run_case


def case(c, d):
    p = os.path.join(d, "u")
    open(p, "wb").close()
    atime, mtime = 1600000000, 1600000123
    os.utime(p, (atime, mtime))
    st = os.stat(p)
    c.check_eq(int(st.st_atime), atime, "atime set")
    c.check_eq(int(st.st_mtime), mtime, "mtime set")


if __name__ == "__main__":
    run_case("20_utime", case)
