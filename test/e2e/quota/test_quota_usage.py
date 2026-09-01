# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Quota accounting tests through the MDS HTTP API."""

import errno
import os

import pytest

pytestmark = pytest.mark.slow
_QUOTA_ERRNOS = {getattr(errno, "EDQUOT", 122), errno.ENOSPC}


def _write_and_sync(path, data, mode="wb"):
    with open(path, mode) as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def test_create_delete_updates_fs_and_root_quota(quota_dir, quota_api):
    """File and directory lifecycle updates filesystem and root usage."""
    before = quota_api.get_fs_quota()
    before_values = quota_api.values(before)
    assert quota_api.values(quota_api.get_dir_quota(quota_api.root_ino)) == before_values

    file_path = os.path.join(quota_dir, "file")
    child_dir = os.path.join(quota_dir, "dir")
    _write_and_sync(file_path, b"a" * 4096)
    os.mkdir(child_dir)

    expected = before_values[:2] + (
        before_values[2] + 4096,
        before_values[3] + 2,
    )
    assert quota_api.values(
        quota_api.wait_for_values(quota_api.get_fs_quota, expected)
    ) == expected
    assert quota_api.values(quota_api.get_dir_quota(quota_api.root_ino)) == expected

    os.unlink(file_path)
    os.rmdir(child_dir)
    assert quota_api.values(
        quota_api.wait_for_values(quota_api.get_fs_quota, before_values)
    ) == before_values


def test_file_size_lifecycle_updates_quota(quota_dir, quota_api):
    before = quota_api.get_fs_quota()
    before_values = quota_api.values(before)
    path = os.path.join(quota_dir, "lifecycle")
    sparse = os.path.join(quota_dir, "sparse")

    _write_and_sync(path, b"x" * 8192)
    expected = before_values[:2] + (before_values[2] + 8192, before_values[3] + 1)
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected

    _write_and_sync(path, b"y" * 8192, "r+b")
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected

    os.truncate(path, 4096)
    expected = before_values[:2] + (before_values[2] + 4096, before_values[3] + 1)
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected

    os.truncate(path, 12288)
    expected = before_values[:2] + (before_values[2] + 12288, before_values[3] + 1)
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected

    open(sparse, "wb").close()
    os.truncate(sparse, (1024 * 1024) + 1)
    expected = before_values[:2] + (
        before_values[2] + 12288 + (1024 * 1024) + 1,
        before_values[3] + 2,
    )
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected

    os.unlink(path)
    os.unlink(sparse)
    assert quota_api.values(
        quota_api.wait_for_values(quota_api.get_fs_quota, before_values)
    ) == before_values


def test_links_rename_and_types_update_quota(quota_dir, quota_api):
    before_values = quota_api.values(quota_api.get_fs_quota())
    source = os.path.join(quota_dir, "source")
    renamed = os.path.join(quota_dir, "renamed")
    hardlink = os.path.join(quota_dir, "hardlink")
    symlink = os.path.join(quota_dir, "symlink")
    subdir = os.path.join(quota_dir, "subdir")

    _write_and_sync(source, b"q" * 1024)
    after_file = before_values[:2] + (before_values[2] + 1024, before_values[3] + 1)
    quota_api.wait_for_values(quota_api.get_fs_quota, after_file)

    os.link(source, hardlink)
    os.rename(source, renamed)
    os.symlink("renamed", symlink)
    os.mkdir(subdir)
    expected = after_file[:2] + (after_file[2], after_file[3] + 2)
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected
    assert os.stat(renamed).st_ino == os.stat(hardlink).st_ino

    os.unlink(hardlink)
    assert quota_api.values(quota_api.wait_for_values(quota_api.get_fs_quota, expected)) == expected
    os.unlink(renamed)
    os.unlink(symlink)
    os.rmdir(subdir)
    assert quota_api.values(
        quota_api.wait_for_values(quota_api.get_fs_quota, before_values)
    ) == before_values


def test_directory_quota_rejects_excess_usage(quota_dir, quota_api):
    directory = os.path.join(quota_dir, "limited")
    os.mkdir(directory)
    inode = os.stat(directory).st_ino
    max_bytes, max_inodes = 8192, 2
    quota_api.set_dir_quota(inode, max_bytes, max_inodes)
    try:
        expected = (max_bytes, max_inodes, 0, 0)
        assert quota_api.values(quota_api.wait_for(
            lambda: quota_api.get_dir_quota(inode),
            lambda quota: quota_api.values(quota)[:2] == expected[:2],
        )) == expected

        first = os.path.join(directory, "first")
        _write_and_sync(first, b"a" * 4096)
        expected = (max_bytes, max_inodes, 4096, 1)
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(inode), expected
        )) == expected
        assert quota_api.values(quota_api.get_dir_quota(os.stat(first).st_ino)) == expected

        with pytest.raises(OSError) as exc:
            _write_and_sync(first, b"b" * 4097, "ab")
        assert exc.value.errno in _QUOTA_ERRNOS
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(inode), expected
        )) == expected

        second = os.path.join(directory, "second")
        open(second, "wb").close()
        expected = (max_bytes, max_inodes, 4096, 2)
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(inode), expected
        )) == expected

        with pytest.raises(OSError) as exc:
            open(os.path.join(directory, "third"), "wb").close()
        assert exc.value.errno in _QUOTA_ERRNOS
        assert quota_api.values(quota_api.get_dir_quota(inode)) == expected
        assert quota_api.values(quota_api.get_dir_quota(inode))[0] != quota_api.values(
            quota_api.get_fs_quota()
        )[0]

        os.unlink(first)
        os.unlink(second)
    finally:
        quota_api.delete_dir_quota(inode)
        os.rmdir(directory)


def test_nested_directory_quotas_and_rename(quota_dir, quota_api):
    outer = os.path.join(quota_dir, "outer")
    inner = os.path.join(outer, "inner")
    os.mkdir(outer)
    outer_inode = os.stat(outer).st_ino
    quota_api.set_dir_quota(outer_inode, 1024 * 1024, 100)
    os.mkdir(inner)
    inner_inode = os.stat(inner).st_ino
    quota_api.set_dir_quota(inner_inode, 512 * 1024, 10)
    try:
        outer_file = os.path.join(outer, "outer-file")
        inner_file = os.path.join(inner, "inner-file")
        _write_and_sync(outer_file, b"o" * 100)
        _write_and_sync(inner_file, b"i" * 2000)

        outer_expected = (1024 * 1024, 100, 2100, 3)
        inner_expected = (512 * 1024, 10, 2000, 1)
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(outer_inode), outer_expected
        )) == outer_expected
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(inner_inode), inner_expected
        )) == inner_expected
        assert quota_api.values(
            quota_api.get_dir_quota(os.stat(inner_file).st_ino)
        ) == inner_expected

        moved = os.path.join(inner, "moved")
        os.rename(outer_file, moved)
        moved_expected = (512 * 1024, 10, 2100, 2)
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(outer_inode), outer_expected
        )) == outer_expected
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(inner_inode), moved_expected
        )) == moved_expected

        os.rename(moved, outer_file)
        assert quota_api.values(quota_api.wait_for_values(
            lambda: quota_api.get_dir_quota(inner_inode), inner_expected
        )) == inner_expected
    finally:
        for path in (os.path.join(outer, "outer-file"),
                     os.path.join(inner, "inner-file"),
                     os.path.join(inner, "moved")):
            if os.path.exists(path):
                os.unlink(path)
        quota_api.delete_dir_quota(inner_inode)
        quota_api.delete_dir_quota(outer_inode)
        os.rmdir(inner)
        os.rmdir(outer)


def test_quota_api_errors_and_fallback(quota_api):
    fs_quota = quota_api.get_fs_quota()
    first = quota_api.get_fs_quota()
    second = quota_api.get_fs_quota()
    assert quota_api.values(first) == quota_api.values(fs_quota)
    assert quota_api.values(second) == quota_api.values(first)
    assert second.get("version", 0) >= first.get("version", 0)

    unknown_inode = quota_api.root_ino + 987654321
    assert quota_api.values(quota_api.get_dir_quota(unknown_inode)) == quota_api.values(fs_quota)

    assert quota_api.get_error("GetFsQuota", {"fs_id": quota_api.fs_id + 999999})
    assert quota_api.get_error("GetDirQuota", {
        "fs_id": quota_api.fs_id + 999999,
        "ino": quota_api.root_ino,
    })
