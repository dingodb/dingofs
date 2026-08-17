#!/usr/bin/env python3
"""Case 09: random offset pread verification."""
import os
import random
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "rr")
    data = rand_data(8 << 20, seed=9)
    with open(p, "wb") as f:
        f.write(data)
    fd = os.open(p, os.O_RDONLY)
    rnd = random.Random(99)
    try:
        bad = 0
        for i in range(200):
            off = rnd.randrange(0, len(data) - 1)
            n = rnd.randrange(1, min(65536, len(data) - off) + 1)
            if os.pread(fd, n, off) != data[off:off + n]:
                bad += 1
        c.check_eq(bad, 0, "200 random preads correct")
    finally:
        os.close(fd)


if __name__ == "__main__":
    run_case("09_random_read", case)
