#!/usr/bin/env python3
"""Case 18: stat size/mtime/ctime update on write."""
import os
import time
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "s")
    with open(p, "wb") as f:
        f.write(rand_data(1000, seed=18))
    st1 = os.stat(p)
    c.check_eq(st1.st_size, 1000, "initial size")
    time.sleep(1.1)
    with open(p, "ab") as f:
        f.write(rand_data(2000, seed=188))
    st2 = os.stat(p)
    c.check_eq(st2.st_size, 3000, "size after append")
    c.check(st2.st_mtime > st1.st_mtime, "mtime advanced")
    c.check(st2.st_ctime >= st1.st_ctime, "ctime not decreased")


if __name__ == "__main__":
    run_case("18_stat_metadata", case)
