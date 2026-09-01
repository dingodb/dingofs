# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Additional POSIX semantics retained from the standalone regression suite."""

import mmap
import os
import stat
import time

import pytest

pytestmark = pytest.mark.standard


def test_overwrite_middle_preserves_neighbors(test_dir):
    path = os.path.join(test_dir, "overwrite")
    data = bytearray(os.urandom(1024 * 1024))
    with open(path, "wb") as stream:
        stream.write(data)

    offset = 123456
    patch = os.urandom(4096)
    with open(path, "r+b") as stream:
        stream.seek(offset)
        stream.write(patch)
    data[offset:offset + len(patch)] = patch

    with open(path, "rb") as stream:
        assert stream.read() == data


def test_interleaved_read_write_on_one_descriptor(test_dir):
    path = os.path.join(test_dir, "mixed")
    with open(path, "wb") as stream:
        stream.write(b"\x00" * 4096)

    with open(path, "r+b") as stream:
        for index in range(50):
            offset = index * 64
            data = bytes([index]) * 64
            stream.seek(offset)
            stream.write(data)
            stream.seek(offset)
            assert stream.read(len(data)) == data


def test_ftruncate_open_descriptor_then_continue_io(test_dir):
    path = os.path.join(test_dir, "truncate")
    data = os.urandom(200000)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.write(fd, data)
        os.ftruncate(fd, 50000)
        assert os.pread(fd, 50000, 0) == data[:50000]
        os.ftruncate(fd, 100000)
        assert os.pread(fd, 50000, 50000) == b"\x00" * 50000
        os.pwrite(fd, b"tail", 99996)
    finally:
        os.close(fd)

    with open(path, "rb") as stream:
        stream.seek(99996)
        assert stream.read() == b"tail"


def test_write_and_truncate_update_metadata(test_dir):
    path = os.path.join(test_dir, "metadata")
    with open(path, "wb") as stream:
        stream.write(b"a" * 10000)
    before = os.stat(path)

    time.sleep(1.1)
    os.truncate(path, 5000)
    after_truncate = os.stat(path)
    assert after_truncate.st_size == 5000
    assert after_truncate.st_mtime > before.st_mtime

    time.sleep(1.1)
    with open(path, "ab") as stream:
        stream.write(b"b" * 2000)
    after_write = os.stat(path)
    assert after_write.st_size == 7000
    assert after_write.st_mtime > after_truncate.st_mtime
    assert after_write.st_ctime >= after_truncate.st_ctime


@pytest.mark.skipif(os.geteuid() != 0, reason="chown requires root")
def test_chown_updates_owner(test_dir):
    path = os.path.join(test_dir, "owner")
    open(path, "wb").close()
    os.chown(path, 0, 0)
    status = os.stat(path)
    assert status.st_uid == 0
    assert status.st_gid == 0


def test_mmap_read_write(test_dir):
    path = os.path.join(test_dir, "mapped")
    data = os.urandom(65536)
    with open(path, "wb") as stream:
        stream.write(data)

    with open(path, "r+b") as stream:
        try:
            mapped = mmap.mmap(stream.fileno(), len(data))
        except OSError as error:
            pytest.skip(f"mmap is not supported: {error}")
        try:
            assert mapped[:1000] == data[:1000]
            patch = b"M" * 4096
            mapped[5000:9096] = patch
            mapped.flush()
        finally:
            mapped.close()
    with open(path, "rb") as stream:
        actual = stream.read()
    assert actual[5000:9096] == patch


def test_unlink_open_file_remains_accessible(test_dir):
    path = os.path.join(test_dir, "unlinked")
    data = os.urandom(100000)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.write(fd, data)
        os.unlink(path)
        assert not os.path.exists(path)
        assert os.pread(fd, len(data), 0) == data
        os.pwrite(fd, b"XYZ", 10)
        assert os.pread(fd, 3, 10) == b"XYZ"
    finally:
        os.close(fd)
    assert not os.path.exists(path)


@pytest.mark.slow
def test_listdir_many_files(test_dir):
    count = int(os.environ.get("DINGOFS_MANY_FILES", "10000"))
    names = {f"f{index:06d}" for index in range(count)}
    for name in names:
        open(os.path.join(test_dir, name), "wb").close()

    assert set(os.listdir(test_dir)) == names
    for name in sorted(names)[:100]:
        os.unlink(os.path.join(test_dir, name))
    assert len(os.listdir(test_dir)) == count - min(count, 100)


def test_deep_nested_directories(test_dir):
    path = test_dir
    for index in range(50):
        path = os.path.join(path, f"d{index}")
    os.makedirs(path)
    leaf = os.path.join(path, "leaf")
    with open(leaf, "wb") as stream:
        stream.write(b"deep")
    with open(leaf, "rb") as stream:
        assert stream.read() == b"deep"
    assert sum(len(dirs) for _, dirs, _ in os.walk(test_dir)) == 50


def test_special_file_names(test_dir):
    names = ["space name", "中文", "!@#$%^&()", "x" * 255]
    for name in names:
        with open(os.path.join(test_dir, name), "wb") as stream:
            stream.write(name.encode())
    assert set(os.listdir(test_dir)) == set(names)
    for name in names:
        os.unlink(os.path.join(test_dir, name))
    assert os.listdir(test_dir) == []


def test_rename_directory_preserves_nested_content(test_dir):
    source = os.path.join(test_dir, "source")
    target = os.path.join(test_dir, "target")
    os.makedirs(os.path.join(source, "inner"))
    expected = {f"f{index}": os.urandom(1024) for index in range(10)}
    for name, data in expected.items():
        with open(os.path.join(source, name), "wb") as stream:
            stream.write(data)
    with open(os.path.join(source, "inner", "child"), "wb") as stream:
        stream.write(b"child")

    os.rename(source, target)
    assert not os.path.exists(source)
    for name, data in expected.items():
        with open(os.path.join(target, name), "rb") as stream:
            assert stream.read() == data
    with open(os.path.join(target, "inner", "child"), "rb") as stream:
        assert stream.read() == b"child"


def test_chmod_updates_permissions(test_dir):
    path = os.path.join(test_dir, "permissions")
    open(path, "wb").close()
    for mode in (0o640, 0o755):
        os.chmod(path, mode)
        assert stat.S_IMODE(os.stat(path).st_mode) == mode
