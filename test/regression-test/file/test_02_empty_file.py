#!/usr/bin/env python3
"""Case 02: create 0-byte file, read returns empty."""
import os
from common import run_case


def case(c, d):
    p = os.path.join(d, "empty")
    open(p, "wb").close()
    c.check(os.path.exists(p), "file exists")
    c.check_eq(os.path.getsize(p), 0, "size is 0")
    with open(p, "rb") as f:
        c.check_eq(f.read(), b"", "read returns empty")


if __name__ == "__main__":
    run_case("02_empty_file", case)
