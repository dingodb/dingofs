#!/usr/bin/env python3
"""Case 49: filename longer than 255 bytes raises ENAMETOOLONG."""
import errno
import os
from common import run_case


def case(c, d):
    name = "x" * 300
    p = os.path.join(d, name)
    c.check_raises([errno.ENAMETOOLONG],
                   lambda: os.open(p, os.O_CREAT | os.O_WRONLY, 0o644),
                   "create 300-char name raises ENAMETOOLONG")
    c.check_raises([errno.ENAMETOOLONG], lambda: os.mkdir(p),
                   "mkdir 300-char name raises ENAMETOOLONG")
    # 255 should be fine
    ok = os.path.join(d, "y" * 255)
    open(ok, "wb").close()
    c.check(os.path.exists(ok), "255-char name works")


if __name__ == "__main__":
    run_case("49_enametoolong", case)
