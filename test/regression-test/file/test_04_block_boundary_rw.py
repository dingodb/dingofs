#!/usr/bin/env python3
"""Case 04: read/write files at block boundary sizes (4K/64K/1M/4M +/- 1)."""
import os
from common import run_case, rand_data, md5, md5_file


def case(c, d):
    bases = [4096, 65536, 1 << 20, 4 << 20]
    for base in bases:
        for size in (base - 1, base, base + 1):
            p = os.path.join(d, "b_%d" % size)
            data = rand_data(size, seed=size)
            with open(p, "wb") as f:
                f.write(data)
            c.check_eq(os.path.getsize(p), size, "size %d" % size)
            c.check_eq(md5_file(p), md5(data), "md5 for size %d" % size)


if __name__ == "__main__":
    run_case("04_block_boundary_rw", case)
