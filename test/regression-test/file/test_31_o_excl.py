#!/usr/bin/env python3
"""Case 31: O_CREAT|O_EXCL fails when file exists."""
import errno
import os
from common import run_case


def case(c, d):
    p = os.path.join(d, "x")
    fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    os.close(fd)
    c.check(os.path.exists(p), "O_EXCL create succeeds when absent")
    c.check_raises([errno.EEXIST],
                   lambda: os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644),
                   "O_EXCL on existing file raises EEXIST")


if __name__ == "__main__":
    run_case("31_o_excl", case)
