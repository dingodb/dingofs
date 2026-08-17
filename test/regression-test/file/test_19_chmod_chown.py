#!/usr/bin/env python3
"""Case 19: chmod (and chown if root)."""
import os
import stat
from common import run_case


def case(c, d):
    p = os.path.join(d, "perm")
    open(p, "wb").close()
    os.chmod(p, 0o640)
    c.check_eq(stat.S_IMODE(os.stat(p).st_mode), 0o640, "chmod 640")
    os.chmod(p, 0o755)
    c.check_eq(stat.S_IMODE(os.stat(p).st_mode), 0o755, "chmod 755")
    if os.geteuid() == 0:
        os.chown(p, 1000, 1000)
        st = os.stat(p)
        c.check_eq((st.st_uid, st.st_gid), (1000, 1000), "chown 1000:1000")
    else:
        print("  [SKIP] chown (not root)")


if __name__ == "__main__":
    run_case("19_chmod_chown", case)
