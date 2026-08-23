#!/usr/bin/env python3
"""Case 06: API error handling, fallback for an unknown inode and stable reads."""
from common import (FS_ID, ROOT_INO, get_dir_quota, get_error, get_fs_quota,
                    quota_tuple, quota_version, run_case)


def case(c, d):
    del d  # this case only exercises the MDS query APIs
    fs_quota = get_fs_quota()
    first = get_fs_quota()
    second = get_fs_quota()
    c.check_eq(quota_tuple(first), quota_tuple(fs_quota),
               "repeated GetFsQuota reads return the same quota")
    c.check(quota_version(second) >= quota_version(first),
            "GetFsQuota version is monotonic")

    # GetDirQuota deliberately falls back to the FS quota when no local quota
    # exists, even if the inode does not exist.
    unknown_ino = ROOT_INO + 987654321
    c.check_eq(quota_tuple(get_dir_quota(unknown_ino)), quota_tuple(fs_quota),
               "GetDirQuota falls back for an inode without a local quota")

    bad_fs = FS_ID + 999999
    fs_error = get_error("/GetFsQuota", {"fs_id": bad_fs})
    c.check(fs_error is not None, "GetFsQuota reports an unknown filesystem")
    dir_error = get_error("/GetDirQuota", {"fs_id": bad_fs, "ino": ROOT_INO})
    c.check(dir_error is not None, "GetDirQuota reports an unknown filesystem")


if __name__ == "__main__":
    run_case("06_api_edges", case)
