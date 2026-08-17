#!/usr/bin/env python3
"""Case 46: open a directory O_WRONLY raises EISDIR."""
import errno
import os
from common import run_case


def case(c, d):
    sub = os.path.join(d, "adir")
    os.mkdir(sub)
    c.check_raises([errno.EISDIR], lambda: os.open(sub, os.O_WRONLY),
                   "open(dir, O_WRONLY) raises EISDIR")
    c.check_raises([errno.EISDIR], lambda: os.open(sub, os.O_RDWR),
                   "open(dir, O_RDWR) raises EISDIR")


if __name__ == "__main__":
    run_case("46_open_dir_wronly", case)
