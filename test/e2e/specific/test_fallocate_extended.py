# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Extended fallocate(2) boundary and error coverage."""

import errno
import os

import pytest

from _linux import (FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_KEEP_SIZE,
                    FALLOC_FL_PUNCH_HOLE, FALLOC_FL_ZERO_RANGE, fallocate)

_BLOCK = 4096
_CHUNK = 64 * 1024 * 1024


def _read(path, offset=0, length=-1):
    with open(path, "rb") as stream:
        stream.seek(offset)
        return stream.read(length)


def test_plain_fallocate_preserves_position_and_fills_gaps(test_dir):
    path = os.path.join(test_dir, "plain")
    prefix = os.urandom(_BLOCK)
    with open(path, "wb") as stream:
        stream.write(prefix)

    fd = os.open(path, os.O_RDWR)
    try:
        os.lseek(fd, 123, os.SEEK_SET)
        assert fallocate(fd, 0, 2 * _BLOCK, 2 * _BLOCK)
        assert os.lseek(fd, 0, os.SEEK_CUR) == 123
    finally:
        os.close(fd)

    assert os.path.getsize(path) == 4 * _BLOCK
    assert _read(path, 0, _BLOCK) == prefix
    assert _read(path, _BLOCK, 3 * _BLOCK) == b"\x00" * (3 * _BLOCK)


def test_fallocate_keep_size_punch_and_zero_ranges(test_dir):
    path = os.path.join(test_dir, "ranges")
    data = bytes((index // _BLOCK) % 4 + 65 for index in range(8 * _BLOCK))
    with open(path, "wb") as stream:
        stream.write(data)

    fd = os.open(path, os.O_RDWR)
    try:
        assert fallocate(fd, FALLOC_FL_KEEP_SIZE, 9 * _BLOCK, _BLOCK)
        assert os.fstat(fd).st_size == len(data)
        assert os.pread(fd, len(data), 0) == data

        offset = _BLOCK + 123
        length = 2 * _BLOCK + 257
        assert fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                         offset, length)
        assert os.fstat(fd).st_size == len(data)
        assert _read(path, 0, offset) == data[:offset]
        assert _read(path, offset, length) == b"\x00" * length
        assert _read(path, offset + length, len(data) - offset - length) == data[offset + length:]

        offset = 5 * _BLOCK + 37
        length = _BLOCK + 101
        assert fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
                         offset, length)
        assert os.fstat(fd).st_size == len(data)
        assert _read(path, offset, length) == b"\x00" * length
    finally:
        os.close(fd)


def test_zero_range_extends_and_reopen_observes_change(test_dir):
    path = os.path.join(test_dir, "zero")
    prefix = os.urandom(2 * _BLOCK)
    with open(path, "wb") as stream:
        stream.write(prefix)
    offset = 3 * _BLOCK + 17
    length = 2 * _BLOCK + 29

    fd = os.open(path, os.O_RDWR)
    try:
        assert fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, length)
        assert os.pread(fd, length, offset) == b"\x00" * length
        os.fsync(fd)
    finally:
        os.close(fd)

    assert os.path.getsize(path) == offset + length
    assert _read(path, 0, len(prefix)) == prefix
    assert _read(path, len(prefix), offset + length - len(prefix)) == b"\x00" * (offset + length - len(prefix))


def test_fallocate_rejects_invalid_arguments_and_descriptors(test_dir):
    path = os.path.join(test_dir, "errors")
    with open(path, "wb") as stream:
        stream.write(os.urandom(_BLOCK))

    original = _read(path)
    fd = os.open(path, os.O_RDWR)
    try:
        for mode in (0, FALLOC_FL_KEEP_SIZE,
                     FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                     FALLOC_FL_ZERO_RANGE):
            try:
                fallocate(fd, mode, _BLOCK // 2, 0)
            except OSError as error:
                assert error.errno == errno.EINVAL
        assert os.fstat(fd).st_size == len(original)
        assert os.pread(fd, len(original), 0) == original
        assert not fallocate(fd, FALLOC_FL_PUNCH_HOLE, 0, _BLOCK)
        assert not fallocate(fd, 0x04, 0, _BLOCK)
        for offset, length in ((-1, _BLOCK), (0, -1)):
            with pytest.raises(OSError) as exc:
                fallocate(fd, 0, offset, length)
            assert exc.value.errno == errno.EINVAL
    finally:
        os.close(fd)

    fd = os.open(path, os.O_RDONLY)
    try:
        with pytest.raises(OSError) as exc:
            fallocate(fd, 0, 0, _BLOCK)
        assert exc.value.errno == errno.EBADF
    finally:
        os.close(fd)

    fd = os.open(test_dir, os.O_RDONLY)
    try:
        with pytest.raises(OSError) as exc:
            fallocate(fd, 0, 0, _BLOCK)
        assert exc.value.errno in {errno.EBADF, errno.EINVAL, errno.EISDIR}
    finally:
        os.close(fd)


@pytest.mark.slow
def test_fallocate_at_and_collapse_chunk_boundary(test_dir):
    path = os.path.join(test_dir, "collapse")
    with open(path, "wb") as stream:
        stream.truncate(4 * _CHUNK)

    fd = os.open(path, os.O_RDWR)
    try:
        markers = [f"chunk-{index}".encode() for index in range(4)]
        for index, marker in enumerate(markers):
            os.pwrite(fd, marker, index * _CHUNK)
        assert fallocate(fd, 0, 4 * _CHUNK, 2 * _BLOCK)
        assert os.fstat(fd).st_size == 4 * _CHUNK + 2 * _BLOCK

        os.lseek(fd, 123, os.SEEK_SET)
        if not fallocate(fd, FALLOC_FL_COLLAPSE_RANGE, _CHUNK, _CHUNK):
            pytest.skip("FALLOC_FL_COLLAPSE_RANGE is not supported")
        assert os.lseek(fd, 0, os.SEEK_CUR) == 123
        assert os.fstat(fd).st_size == 3 * _CHUNK + 2 * _BLOCK
        assert os.pread(fd, len(markers[0]), 0) == markers[0]
        assert os.pread(fd, len(markers[2]), _CHUNK) == markers[2]
        assert os.pread(fd, len(markers[3]), 2 * _CHUNK) == markers[3]
        assert not fallocate(fd, FALLOC_FL_COLLAPSE_RANGE, _BLOCK, _CHUNK)
        assert not fallocate(fd, FALLOC_FL_COLLAPSE_RANGE, _CHUNK, 0)
        assert not fallocate(fd, FALLOC_FL_COLLAPSE_RANGE | FALLOC_FL_KEEP_SIZE,
                             0, _CHUNK)
    finally:
        os.close(fd)

    assert _read(path, 0, len(markers[0])) == markers[0]
    assert _read(path, _CHUNK, len(markers[2])) == markers[2]
