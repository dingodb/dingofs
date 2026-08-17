#!/usr/bin/env python3
"""Case 33: O_APPEND semantics - writes land at EOF even after seek."""
import os
from common import run_case


def case(c, d):
    p = os.path.join(d, "x")
    with open(p, "wb") as f:
        f.write(b"A" * 100)
    fd = os.open(p, os.O_WRONLY | os.O_APPEND)
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, b"B" * 10)
    finally:
        os.close(fd)
    with open(p, "rb") as f:
        got = f.read()
    c.check_eq(got, b"A" * 100 + b"B" * 10, "append write lands at EOF despite seek")


if __name__ == "__main__":
    run_case("33_o_append_semantics", case)
