#!/usr/bin/env python3
"""Case 02: logical file size deltas from write, overwrite, truncate and sparse files."""
import os
from common import (get_fs_quota, quota_tuple, run_case, wait_for_values,
                    fsync_write)


def case(c, d):
    before = get_fs_quota()
    path = os.path.join(d, "lifecycle")
    sparse = os.path.join(d, "sparse")

    fsync_write(path, b"x" * 8192)
    before_values = quota_tuple(before)
    expected = before_values[:2] + (before_values[2] + 8192,
                                    before_values[3] + 1)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "initial write charges logical file length")

    # An overwrite of the existing range has zero length delta.
    fsync_write(path, b"y" * 8192, "r+b")
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "same-size overwrite does not double-charge bytes")

    os.truncate(path, 4096)
    expected = before_values[:2] + (before_values[2] + 4096,
                                     before_values[3] + 1)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "truncate shrink credits the removed logical bytes")

    os.truncate(path, 12288)
    expected = before_values[:2] + (before_values[2] + 12288,
                                     before_values[3] + 1)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "truncate grow charges the extended logical bytes")

    # Sparse holes still contribute to inode length, not physical allocation.
    open(sparse, "wb").close()
    os.truncate(sparse, (1 << 20) + 1)
    expected = before_values[:2] + (before_values[2] + 12288 + (1 << 20) + 1,
                                     before_values[3] + 2)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, expected)), expected,
               "sparse-file holes are counted by logical length")

    os.unlink(path)
    os.unlink(sparse)
    c.check_eq(quota_tuple(wait_for_values(get_fs_quota, quota_tuple(before))),
               quota_tuple(before), "deleting files credits their final lengths")


if __name__ == "__main__":
    run_case("02_file_size_lifecycle", case)
