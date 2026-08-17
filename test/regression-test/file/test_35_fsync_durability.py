#!/usr/bin/env python3
"""Case 35: flush/fsync durability - reopen and verify."""
import os
from common import run_case, rand_data, md5, md5_file


def case(c, d):
    p = os.path.join(d, "x")
    data = rand_data(2 << 20, seed=35)
    fd = os.open(p, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
        c.check(True, "fsync did not raise")
        try:
            os.fdatasync(fd)
            c.check(True, "fdatasync did not raise")
        except (AttributeError, OSError) as e:
            print("  [SKIP] fdatasync: %s" % e)
    finally:
        os.close(fd)
    c.check_eq(md5_file(p), md5(data), "data intact after fsync+close+reopen")


if __name__ == "__main__":
    run_case("35_fsync_durability", case)
