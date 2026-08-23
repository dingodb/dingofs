#!/usr/bin/env python3
"""Case 04: set a local directory quota and verify GetDirQuota plus EDQUOT."""
import os
from common import (QUOTA_ERRNOS, delete_dir_quota, get_dir_quota, get_fs_quota,
                    quota_tuple, run_case, set_dir_quota, wait_for_quota,
                    fsync_write)


def case(c, d):
    quota_dir = os.path.join(d, "limited")
    os.mkdir(quota_dir)
    ino = os.stat(quota_dir).st_ino
    max_bytes, max_inodes = 8192, 2
    set_dir_quota(ino, max_bytes, max_inodes)
    try:
        q0 = wait_for_quota(lambda: get_dir_quota(ino),
                            lambda q: quota_tuple(q)[:2] == (max_bytes, max_inodes))
        c.check_eq(quota_tuple(q0), (max_bytes, max_inodes, 0, 0),
                   "new directory quota starts with zero local usage")

        first = os.path.join(quota_dir, "first")
        fsync_write(first, b"a" * 4096)
        expected = (max_bytes, max_inodes, 4096, 1)
        c.check_eq(quota_tuple(wait_for_quota(lambda: get_dir_quota(ino),
                                              lambda q: quota_tuple(q) == expected)),
                   expected, "GetDirQuota reports child bytes and inode usage")
        c.check_eq(quota_tuple(get_dir_quota(os.stat(first).st_ino)), expected,
                   "a descendant resolves to its nearest directory quota")

        # A write that would exceed max_bytes must be rejected before charging.
        def over_bytes():
            with open(first, "ab") as stream:
                stream.write(b"b" * 4097)
                stream.flush()
                os.fsync(stream.fileno())

        c.check_raises(QUOTA_ERRNOS, over_bytes,
                       "write beyond directory byte limit returns a quota error")
        c.check_eq(quota_tuple(wait_for_quota(lambda: get_dir_quota(ino),
                                              lambda q: quota_tuple(q) == expected)),
                   expected, "failed byte-limit write leaves usage unchanged")

        second = os.path.join(quota_dir, "second")
        open(second, "wb").close()
        expected = (max_bytes, max_inodes, 4096, 2)
        c.check_eq(quota_tuple(wait_for_quota(lambda: get_dir_quota(ino),
                                              lambda q: quota_tuple(q) == expected)),
                   expected, "inode usage reaches the configured limit")

        def over_inodes():
            open(os.path.join(quota_dir, "third"), "wb").close()

        c.check_raises(QUOTA_ERRNOS, over_inodes,
                       "creating an entry beyond inode limit returns a quota error")
        c.check_eq(quota_tuple(get_dir_quota(ino)), expected,
                   "failed inode-limit create leaves usage unchanged")

        # The local directory limit must not replace the filesystem quota.
        fs_quota = get_fs_quota()
        c.check(quota_tuple(get_dir_quota(ino))[0] != quota_tuple(fs_quota)[0],
                "directory quota has its own limit")

        os.unlink(first)
        os.unlink(second)
    finally:
        delete_dir_quota(ino)
        os.rmdir(quota_dir)


if __name__ == "__main__":
    run_case("04_directory_quota_limits", case)
