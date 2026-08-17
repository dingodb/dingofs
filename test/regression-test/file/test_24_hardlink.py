#!/usr/bin/env python3
"""Case 24: hard link (skips gracefully if unsupported)."""
import errno
import os
from common import run_case, rand_data


def case(c, d):
    a, b = os.path.join(d, "a"), os.path.join(d, "b")
    data = rand_data(50000, seed=24)
    with open(a, "wb") as f:
        f.write(data)
    try:
        os.link(a, b)
    except OSError as e:
        if e.errno in (errno.EPERM, errno.ENOTSUP, errno.ENOSYS):
            print("  [SKIP] hard link not supported (errno=%d)" % e.errno)
            return
        raise
    c.check_eq(os.stat(a).st_nlink, 2, "nlink == 2")
    c.check_eq(os.stat(a).st_ino, os.stat(b).st_ino, "same inode")
    with open(b, "rb") as f:
        c.check_eq(f.read(), data, "link content identical")
    with open(b, "ab") as f:
        f.write(b"XYZ")
    with open(a, "rb") as f:
        c.check_eq(f.read(), data + b"XYZ", "write via link visible via original")
    os.unlink(a)
    c.check_eq(os.stat(b).st_nlink, 1, "nlink back to 1 after unlink")


if __name__ == "__main__":
    run_case("24_hardlink", case)
