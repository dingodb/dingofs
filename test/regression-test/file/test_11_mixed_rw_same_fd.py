#!/usr/bin/env python3
"""Case 11: alternate read/write on the same fd."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "mix")
    with open(p, "wb") as f:
        f.write(rand_data(1 << 20, seed=11))
    ok = True
    with open(p, "r+b") as f:
        for i in range(50):
            off = i * 10000
            buf = rand_data(512, seed=i)
            f.seek(off)
            f.write(buf)
            f.seek(off)
            if f.read(512) != buf:
                ok = False
    c.check(ok, "50 alternating write-then-read on same fd")


if __name__ == "__main__":
    run_case("11_mixed_rw_same_fd", case)
