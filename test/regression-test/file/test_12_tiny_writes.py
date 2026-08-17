#!/usr/bin/env python3
"""Case 12: high-frequency tiny writes (many 1B~64B writes)."""
import os
import random
from common import run_case


def case(c, d):
    p = os.path.join(d, "tiny")
    rnd = random.Random(12)
    parts = []
    with open(p, "wb") as f:
        for i in range(5000):
            buf = bytes([rnd.getrandbits(8)]) * rnd.randrange(1, 65)
            f.write(buf)
            parts.append(buf)
    expect = b"".join(parts)
    with open(p, "rb") as f:
        c.check_eq(f.read() == expect, True, "5000 tiny writes content correct")
    c.check_eq(os.path.getsize(p), len(expect), "size after tiny writes")


if __name__ == "__main__":
    run_case("12_tiny_writes", case)
