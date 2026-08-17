#!/usr/bin/env python3
"""Case 01: write random data then read back, verify md5."""
import os
from common import run_case, rand_data, md5, md5_file


def case(c, d):
    p = os.path.join(d, "f1")
    data = rand_data(8 * 1024 * 1024, seed=1)
    with open(p, "wb") as f:
        f.write(data)
    c.check_eq(os.path.getsize(p), len(data), "file size")
    c.check_eq(md5_file(p), md5(data), "md5 of read-back data")


if __name__ == "__main__":
    run_case("01_write_read_consistency", case)
