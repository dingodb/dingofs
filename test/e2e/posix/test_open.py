# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""POSIX open, descriptor visibility, and error semantics."""

import errno
import os

import pytest

pytestmark = pytest.mark.standard


def test_open_exclusive_existing_fails(test_dir):
    path = os.path.join(test_dir, "exclusive")
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    os.close(fd)

    with pytest.raises(OSError) as exc:
        os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    assert exc.value.errno == errno.EEXIST


def test_open_trunc_and_append_ignore_seek(test_dir):
    path = os.path.join(test_dir, "modes")
    with open(path, "wb") as stream:
        stream.write(b"old")

    fd = os.open(path, os.O_WRONLY | os.O_TRUNC)
    os.close(fd)
    assert os.path.getsize(path) == 0

    with open(path, "wb") as stream:
        stream.write(b"A" * 100)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, b"B" * 10)
    finally:
        os.close(fd)
    with open(path, "rb") as stream:
        assert stream.read() == b"A" * 100 + b"B" * 10


def test_open_modes_text_binary_and_exclusive(test_dir):
    append_path = os.path.join(test_dir, "append.txt")
    with open(append_path, "a") as stream:
        stream.write("first")
        stream.seek(0)
        stream.write("second")
    with open(append_path) as stream:
        assert stream.read() == "firstsecond"

    text_path = os.path.join(test_dir, "text.txt")
    with open(text_path, "wt") as stream:
        stream.write("line1\nline2\n")
    with open(text_path, "rt") as stream:
        assert stream.read() == "line1\nline2\n"
    with open(text_path, "w") as stream:
        stream.write("replacement")
    with open(text_path, "r") as stream:
        assert stream.read() == "replacement"

    binary_path = os.path.join(test_dir, "binary")
    data = os.urandom(100000)
    with open(binary_path, "wb") as stream:
        stream.write(data)
    with open(binary_path, "rb") as stream:
        assert stream.read() == data

    with open(os.path.join(test_dir, "new"), "x"):
        pass


def test_two_descriptors_observe_fsync_and_overwrite(test_dir):
    path = os.path.join(test_dir, "shared")
    open(path, "wb").close()
    writer = os.open(path, os.O_WRONLY)
    reader = os.open(path, os.O_RDONLY)
    try:
        data = os.urandom(65536)
        os.write(writer, data)
        os.fsync(writer)
        assert os.pread(reader, len(data), 0) == data

        patch = os.urandom(1000)
        os.pwrite(writer, patch, 100)
        os.fsync(writer)
        assert os.pread(reader, len(patch), 100) == patch
    finally:
        os.close(writer)
        os.close(reader)


def test_fsync_and_fdatasync_persist_data(test_dir):
    path = os.path.join(test_dir, "synced")
    data = os.urandom(2 * 1024 * 1024)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
        os.fdatasync(fd)
    finally:
        os.close(fd)

    with open(path, "rb") as stream:
        assert stream.read() == data


def test_read_beyond_eof(test_dir):
    path = os.path.join(test_dir, "short")
    data = os.urandom(1000)
    with open(path, "wb") as stream:
        stream.write(data)

    fd = os.open(path, os.O_RDONLY)
    try:
        assert os.pread(fd, 100, 2000) == b""
        assert os.pread(fd, 500, 800) == data[800:]
    finally:
        os.close(fd)

    with open(path, "rb") as stream:
        stream.seek(5000)
        assert stream.read() == b""


def test_open_directory_for_write_fails_eisdir(test_dir):
    path = os.path.join(test_dir, "directory")
    os.mkdir(path)
    for flags in (os.O_WRONLY, os.O_RDWR):
        with pytest.raises(OSError) as exc:
            os.open(path, flags)
        assert exc.value.errno == errno.EISDIR


def test_missing_path_operations_fail_enoent(test_dir):
    path = os.path.join(test_dir, "missing")
    for operation in (
        lambda: os.open(path, os.O_RDONLY),
        lambda: os.open(path, os.O_WRONLY),
        lambda: os.stat(path),
        lambda: os.unlink(path),
    ):
        with pytest.raises(OSError) as exc:
            operation()
        assert exc.value.errno == errno.ENOENT


def test_path_below_file_fails_enotdir(test_dir):
    path = os.path.join(test_dir, "file")
    with open(path, "wb") as stream:
        stream.write(b"x")
    child = os.path.join(path, "child")
    for operation in (
        lambda: os.open(child, os.O_RDONLY),
        lambda: os.open(child, os.O_CREAT | os.O_WRONLY, 0o644),
        lambda: os.mkdir(child),
        lambda: os.listdir(path),
    ):
        with pytest.raises(OSError) as exc:
            operation()
        assert exc.value.errno == errno.ENOTDIR


def test_name_max_boundary(test_dir):
    too_long = os.path.join(test_dir, "x" * 300)
    for operation in (
        lambda: os.open(too_long, os.O_CREAT | os.O_WRONLY, 0o644),
        lambda: os.mkdir(too_long),
    ):
        with pytest.raises(OSError) as exc:
            operation()
        assert exc.value.errno == errno.ENAMETOOLONG

    valid = os.path.join(test_dir, "y" * 255)
    open(valid, "wb").close()
    assert os.path.exists(valid)
