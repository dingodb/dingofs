#!/usr/bin/env python3
"""Case 48: path component is a file (file/xxx) raises ENOTDIR."""
import errno
import os
from common import run_case


def case(c, d):
    f = os.path.join(d, "plainfile")
    with open(f, "wb") as fh:
        fh.write(b"x")
    bad = os.path.join(f, "child")
    c.check_raises([errno.ENOTDIR], lambda: os.open(bad, os.O_RDONLY),
                   "open file/child raises ENOTDIR")
    c.check_raises([errno.ENOTDIR], lambda: os.open(bad, os.O_CREAT | os.O_WRONLY, 0o644),
                   "create file/child raises ENOTDIR")
    c.check_raises([errno.ENOTDIR], lambda: os.mkdir(bad), "mkdir file/child raises ENOTDIR")
    c.check_raises([errno.ENOTDIR], lambda: os.listdir(f), "listdir on file raises ENOTDIR")


if __name__ == "__main__":
    run_case("48_enotdir", case)
