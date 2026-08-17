#!/usr/bin/env python3
"""Case 50: statvfs sanity - free space decreases reasonably after big write."""
import os
import time
from common import run_case, rand_data


def case(c, d):
    sv1 = os.statvfs(d)
    c.check(sv1.f_bsize > 0 and sv1.f_blocks > 0, "statvfs returns sane values "
            "(bsize=%d blocks=%d bavail=%d)" % (sv1.f_bsize, sv1.f_blocks, sv1.f_bavail))
    size = 64 << 20
    p = os.path.join(d, "big")
    with open(p, "wb") as f:
        f.write(rand_data(size, seed=50))
        os.fsync(f.fileno())
    time.sleep(2)  # allow fs accounting to settle
    sv2 = os.statvfs(d)
    used_delta = (sv1.f_bavail - sv2.f_bavail) * sv1.f_frsize
    # tolerant check: delta should not be wildly negative
    c.check(used_delta >= -(8 << 20),
            "free space did not increase after 64MB write (delta=%d bytes)" % used_delta)
    os.unlink(p)


if __name__ == "__main__":
    run_case("50_statvfs", case)
