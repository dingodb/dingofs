#!/usr/bin/env python3
"""Case 27: listdir completeness with many files (default 10000, env DINGOFS_MANY_FILES)."""
import os
from common import run_case


def case(c, d):
    n = int(os.environ.get("DINGOFS_MANY_FILES", "10000"))
    names = set("f%06d" % i for i in range(n))
    for name in names:
        open(os.path.join(d, name), "wb").close()
    listed = set(os.listdir(d))
    c.check_eq(len(listed), n, "listdir count")
    c.check(listed == names, "listdir names complete")
    # cleanup a few and re-check
    for i in range(100):
        os.unlink(os.path.join(d, "f%06d" % i))
    c.check_eq(len(os.listdir(d)), n - 100, "listdir count after deleting 100")


if __name__ == "__main__":
    run_case("27_listdir_many_files", case)
