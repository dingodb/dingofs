#!/usr/bin/env python3
"""Case 03: small file read/write (1B, hundreds of bytes)."""
import os
from common import run_case, rand_data


def case(c, d):
    for size in (1, 2, 100, 511, 512, 777):
        p = os.path.join(d, "small_%d" % size)
        data = rand_data(size, seed=size)
        with open(p, "wb") as f:
            f.write(data)
        with open(p, "rb") as f:
            c.check_eq(f.read(), data, "content of %dB file" % size)


if __name__ == "__main__":
    run_case("03_small_file_rw", case)
