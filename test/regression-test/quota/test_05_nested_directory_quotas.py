#!/usr/bin/env python3
"""Case 05: nested quotas, nearest lookup and cross-directory rename."""
import os
from common import (delete_dir_quota, get_dir_quota, quota_tuple, run_case,
                    set_dir_quota, wait_for_quota, fsync_write)


def wait_exact(ino, expected):
    return wait_for_quota(lambda: get_dir_quota(ino),
                          lambda q: quota_tuple(q) == expected)


def case(c, d):
    outer = os.path.join(d, "outer")
    inner = os.path.join(outer, "inner")
    os.mkdir(outer)
    outer_ino = os.stat(outer).st_ino
    set_dir_quota(outer_ino, 1 << 20, 100)
    os.mkdir(inner)
    inner_ino = os.stat(inner).st_ino
    set_dir_quota(inner_ino, 512 << 10, 10)
    try:
        outer_file = os.path.join(outer, "outer-file")
        inner_file = os.path.join(inner, "inner-file")
        fsync_write(outer_file, b"o" * 100)
        fsync_write(inner_file, b"i" * 2000)

        outer_expected = (1 << 20, 100, 2100, 3)  # outer-file, inner dir, inner-file
        inner_expected = (512 << 10, 10, 2000, 1)
        c.check_eq(quota_tuple(wait_exact(outer_ino, outer_expected)), outer_expected,
                   "outer quota includes the whole nested subtree")
        c.check_eq(quota_tuple(wait_exact(inner_ino, inner_expected)), inner_expected,
                   "inner quota only includes its own subtree")
        c.check_eq(quota_tuple(get_dir_quota(os.stat(inner_file).st_ino)), inner_expected,
                   "descendant lookup returns the nearest quota, not outer quota")

        moved = os.path.join(inner, "moved")
        os.rename(outer_file, moved)
        outer_after_move = outer_expected
        inner_after_move = (512 << 10, 10, 2100, 2)
        c.check_eq(quota_tuple(wait_exact(outer_ino, outer_after_move)), outer_after_move,
                   "moving a file into inner keeps aggregate outer usage stable")
        c.check_eq(quota_tuple(wait_exact(inner_ino, inner_after_move)), inner_after_move,
                   "moving a file into inner charges the inner quota")

        os.rename(moved, outer_file)
        c.check_eq(quota_tuple(wait_exact(inner_ino, inner_expected)), inner_expected,
                   "moving the file back credits the inner quota")
    finally:
        for path in (os.path.join(outer, "outer-file"),
                     os.path.join(inner, "inner-file"),
                     os.path.join(inner, "moved")):
            if os.path.exists(path):
                os.unlink(path)
        delete_dir_quota(inner_ino)
        delete_dir_quota(outer_ino)
        os.rmdir(inner)
        os.rmdir(outer)


if __name__ == "__main__":
    run_case("05_nested_directory_quotas", case)
