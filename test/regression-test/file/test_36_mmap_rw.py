#!/usr/bin/env python3
"""Case 36: mmap read/write (skips if unsupported)."""
import errno
import mmap
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "x")
    size = 1 << 20
    data = rand_data(size, seed=36)
    with open(p, "wb") as f:
        f.write(data)
    with open(p, "r+b") as f:
        try:
            m = mmap.mmap(f.fileno(), size)
        except (OSError, ValueError) as e:
            if isinstance(e, OSError) and e.errno in (errno.ENODEV, errno.ENOSYS, errno.ENOTSUP):
                print("  [SKIP] mmap not supported: %s" % e)
                return
            raise
        try:
            c.check_eq(bytes(m[:1000]), data[:1000], "mmap read matches")
            patch = rand_data(4096, seed=366)
            m[5000:5000 + 4096] = patch
            m.flush()
        finally:
            m.close()
    with open(p, "rb") as f:
        got = f.read()
    c.check_eq(got[5000:9096], patch, "mmap write visible via read()")
    c.check_eq(got[:5000], data[:5000], "prefix unchanged")


if __name__ == "__main__":
    run_case("36_mmap_rw", case)
