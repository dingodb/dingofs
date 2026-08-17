#!/usr/bin/env python3
"""Case 30: special file names: chinese, spaces, 255-byte name, special chars."""
import os
from common import run_case


def case(c, d):
    names = [
        "中文文件名.txt",
        "name with spaces .log",
        "dots...and---dashes___",
        "tab\tname" if os.name != "nt" else "tabname",
        "quo'te\"s",
        "!@#$%^&()+=[]{};,",
        "x" * 255,
        "中" * 85,  # 255 bytes utf-8
    ]
    for name in names:
        p = os.path.join(d, name)
        payload = name.encode("utf-8")
        with open(p, "wb") as f:
            f.write(payload)
        with open(p, "rb") as f:
            c.check_eq(f.read(), payload, "rw file %r" % name[:20])
    listed = set(os.listdir(d))
    c.check_eq(len(listed), len(names), "listdir shows all special names")
    for name in names:
        os.unlink(os.path.join(d, name))
    c.check_eq(os.listdir(d), [], "all special-name files removed")


if __name__ == "__main__":
    run_case("30_special_filenames", case)
