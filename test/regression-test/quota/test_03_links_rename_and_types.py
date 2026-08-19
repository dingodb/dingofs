#!/usr/bin/env python3
"""Case 03: inode accounting for hard links, symlinks, rename and directories."""
import os
from common import get_fs_quota, quota_tuple, run_case, wait_for_values, fsync_write


def case(c, d):
    before = get_fs_quota()
    source = os.path.join(d, "source")
    renamed = os.path.join(d, "renamed")
    hardlink = os.path.join(d, "hardlink")
    symlink = os.path.join(d, "symlink")
    subdir = os.path.join(d, "subdir")

    fsync_write(source, b"q" * 1024)
    before_values = quota_tuple(before)
    after_file = before_values[:2] + (before_values[2] + 1024,
                                      before_values[3] + 1)
    wait_for_values(get_fs_quota, after_file)

    os.link(source, hardlink)
    os.rename(source, renamed)
    os.symlink("renamed", symlink)
    os.mkdir(subdir)
    expected = after_file[:2] + (after_file[2], after_file[3] + 2)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "hard link and rename preserve FS usage; symlink and mkdir add inodes")

    # A hard link is not a second filesystem inode or a second byte payload.
    c.check_eq(os.stat(renamed).st_ino, os.stat(hardlink).st_ino,
               "hard-link paths refer to one inode")

    os.unlink(hardlink)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "removing a non-final hard link does not release the inode")
    os.unlink(renamed)
    os.unlink(symlink)
    os.rmdir(subdir)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, quota_tuple(before))),
               quota_tuple(before), "removing final entries restores FS usage")


if __name__ == "__main__":
    run_case("03_links_rename_and_types", case)
