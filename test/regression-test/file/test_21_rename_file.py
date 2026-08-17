#!/usr/bin/env python3
"""Case 21: rename file, content unchanged, old path gone."""
import os
from common import run_case, rand_data, md5, md5_file


def case(c, d):
    src, dst = os.path.join(d, "a"), os.path.join(d, "b")
    data = rand_data(300000, seed=21)
    with open(src, "wb") as f:
        f.write(data)
    os.rename(src, dst)
    c.check(not os.path.exists(src), "old path gone")
    c.check(os.path.exists(dst), "new path exists")
    c.check_eq(md5_file(dst), md5(data), "content unchanged after rename")


if __name__ == "__main__":
    run_case("21_rename_file", case)
