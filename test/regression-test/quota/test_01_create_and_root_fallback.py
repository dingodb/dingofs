#!/usr/bin/env python3
"""Case 01: file/directory creation and GetDirQuota's filesystem fallback."""
import os
from common import (ROOT_INO, get_dir_quota, get_fs_quota, quota_tuple,
                    run_case, wait_for_values, fsync_write)


def case(c, d):
    before = get_fs_quota()
    root_before = get_dir_quota(ROOT_INO)
    c.check_eq(quota_tuple(root_before), quota_tuple(before),
               "root GetDirQuota falls back to the filesystem quota")

    file_path = os.path.join(d, "file")
    child_dir = os.path.join(d, "dir")
    fsync_write(file_path, b"a" * 4096)
    os.mkdir(child_dir)

    before_values = quota_tuple(before)
    expected = before_values[:2] + (before_values[2] + 4096,
                                    before_values[3] + 2)
    after = wait_for_values(get_fs_quota, expected)
    c.check_eq(quota_tuple(after), expected,
               "creating a file and directory updates FS bytes/inodes")
    c.check_eq(quota_tuple(get_dir_quota(ROOT_INO)), expected,
               "root directory quota reports the same usage")

    os.unlink(file_path)
    os.rmdir(child_dir)
    restored = wait_for_values(get_fs_quota, quota_tuple(before))
    c.check_eq(quota_tuple(restored), quota_tuple(before),
               "deleting created entries restores FS usage")


if __name__ == "__main__":
    run_case("01_create_and_root_fallback", case)
