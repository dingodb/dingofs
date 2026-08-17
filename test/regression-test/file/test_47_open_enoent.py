#!/usr/bin/env python3
"""Case 47: open nonexistent file without O_CREAT raises ENOENT."""
import errno
import os
from common import run_case


def case(c, d):
    p = os.path.join(d, "nope")
    c.check_raises([errno.ENOENT], lambda: os.open(p, os.O_RDONLY),
                   "open missing file RDONLY raises ENOENT")
    c.check_raises([errno.ENOENT], lambda: os.open(p, os.O_WRONLY),
                   "open missing file WRONLY raises ENOENT")
    c.check_raises([errno.ENOENT], lambda: os.stat(p), "stat missing file raises ENOENT")
    c.check_raises([errno.ENOENT], lambda: os.unlink(p), "unlink missing file raises ENOENT")


if __name__ == "__main__":
    run_case("47_open_enoent", case)
