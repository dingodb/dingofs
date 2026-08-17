#!/usr/bin/env python3
"""Case 26: mkdir/rmdir; rmdir on non-empty dir raises ENOTEMPTY."""
import errno
import os
from common import run_case


def case(c, d):
    sub = os.path.join(d, "sub")
    os.mkdir(sub)
    c.check(os.path.isdir(sub), "mkdir works")
    open(os.path.join(sub, "f"), "wb").close()
    c.check_raises([errno.ENOTEMPTY, errno.EEXIST], lambda: os.rmdir(sub),
                   "rmdir non-empty raises ENOTEMPTY")
    os.unlink(os.path.join(sub, "f"))
    os.rmdir(sub)
    c.check(not os.path.exists(sub), "rmdir empty dir works")


if __name__ == "__main__":
    run_case("26_mkdir_rmdir", case)
