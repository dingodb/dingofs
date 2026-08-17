#!/usr/bin/env python3
"""Case 07: overwrite in the middle, surrounding regions unchanged."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "ow")
    data = bytearray(rand_data(1 << 20, seed=7))
    with open(p, "wb") as f:
        f.write(data)
    patch = rand_data(4096, seed=77)
    off = 123456
    with open(p, "r+b") as f:
        f.seek(off)
        f.write(patch)
    data[off:off + len(patch)] = patch
    with open(p, "rb") as f:
        got = f.read()
    c.check_eq(len(got), len(data), "size unchanged")
    c.check_eq(got[:off], bytes(data[:off]), "prefix unchanged")
    c.check_eq(got[off:off + 4096], patch, "patched region")
    c.check_eq(got[off + 4096:], bytes(data[off + 4096:]), "suffix unchanged")


if __name__ == "__main__":
    run_case("07_overwrite_middle", case)
