#!/usr/bin/env python3
"""Case 10: random offset pwrite, then verify whole file."""
import os
import random
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "rw")
    size = 8 << 20
    data = bytearray(size)
    with open(p, "wb") as f:
        f.write(bytes(size))
    fd = os.open(p, os.O_WRONLY)
    rnd = random.Random(10)
    try:
        for i in range(200):
            off = rnd.randrange(0, size - 1)
            n = rnd.randrange(1, min(65536, size - off) + 1)
            buf = rand_data(n, seed=1000 + i)
            os.pwrite(fd, buf, off)
            data[off:off + n] = buf
    finally:
        os.close(fd)
    with open(p, "rb") as f:
        c.check_eq(f.read(), bytes(data), "file matches expected after 200 random pwrites")


if __name__ == "__main__":
    run_case("10_random_write", case)
