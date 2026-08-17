#!/usr/bin/env python3
"""Case 17: mtime/size metadata updated by truncate."""
import os
import time
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "t")
    with open(p, "wb") as f:
        f.write(rand_data(100000, seed=17))
    st1 = os.stat(p)
    time.sleep(1.1)
    os.truncate(p, 5000)
    st2 = os.stat(p)
    c.check_eq(st2.st_size, 5000, "size updated")
    c.check(st2.st_mtime > st1.st_mtime, "mtime updated by truncate (%.2f -> %.2f)" % (st1.st_mtime, st2.st_mtime))


if __name__ == "__main__":
    run_case("17_truncate_metadata", case)
