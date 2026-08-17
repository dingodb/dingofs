#!/usr/bin/env python3
"""Case 25: symlink create, readlink, read/write through symlink."""
import errno
import os
from common import run_case, rand_data


def case(c, d):
    target, ln = os.path.join(d, "target"), os.path.join(d, "ln")
    data = rand_data(30000, seed=25)
    with open(target, "wb") as f:
        f.write(data)
    try:
        os.symlink(target, ln)
    except OSError as e:
        if e.errno in (errno.EPERM, errno.ENOTSUP, errno.ENOSYS):
            print("  [SKIP] symlink not supported (errno=%d)" % e.errno)
            return
        raise
    c.check(os.path.islink(ln), "islink true")
    c.check_eq(os.readlink(ln), target, "readlink value")
    with open(ln, "rb") as f:
        c.check_eq(f.read(), data, "read through symlink")
    with open(ln, "ab") as f:
        f.write(b"tail")
    with open(target, "rb") as f:
        c.check_eq(f.read(), data + b"tail", "write through symlink visible on target")


if __name__ == "__main__":
    run_case("25_symlink", case)
