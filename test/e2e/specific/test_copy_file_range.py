# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""copy_file_range(2) semantics through the mounted DingoFS client."""

import errno
import os
import random
import threading
import time

import pytest

from _linux import copy_file_range

_BLOCK = 4096
_CHUNK = 64 * 1024 * 1024


def _data(size, seed):
    """Match the legacy suite's deterministic data pattern."""
    generator = random.Random(seed)
    chunk = bytes(generator.getrandbits(8) for _ in range(min(size, 65536)))
    return (chunk * (size // len(chunk) + 1))[:size] if chunk else b""


def _copy(*args, **kwargs):
    try:
        return copy_file_range(*args, **kwargs)
    except NotImplementedError as error:
        pytest.skip(str(error))


def _write(path, data):
    with open(path, "wb") as stream:
        stream.write(data)


def _read(path, offset=0, length=-1):
    with open(path, "rb") as stream:
        stream.seek(offset)
        return stream.read(length)


def test_copy_file_range_basic_and_explicit_offsets(test_dir):
    source = os.path.join(test_dir, "source")
    target = os.path.join(test_dir, "target")
    data = _data(5 * _BLOCK + 37, 6401)
    _write(source, data)
    open(target, "wb").close()

    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, len(data), 0, 0) == len(data)
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert _read(target) == data
    assert os.path.getsize(target) == len(data)

    original = b"D" * (6 * _BLOCK)
    _write(target, original)
    source_offset = _BLOCK + 11
    target_offset = 2 * _BLOCK + 19
    length = 3 * _BLOCK + 23
    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, length, source_offset, target_offset) == length
    finally:
        os.close(source_fd)
        os.close(target_fd)
    expected = original[:target_offset] + data[source_offset:source_offset + length] + original[target_offset + length:]
    assert _read(target) == expected
    assert os.path.getsize(target) == len(original)


def test_copy_file_range_extends_and_clamps_at_eof(test_dir):
    source = os.path.join(test_dir, "source")
    target = os.path.join(test_dir, "target")
    data = _data(2 * _BLOCK + 31, 6402)
    _write(source, data)
    _write(target, b"prefix")
    target_offset = 3 * _BLOCK + 7

    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, len(data), 0, target_offset) == len(data)
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert os.path.getsize(target) == target_offset + len(data)
    assert _read(target, 0, 6) == b"prefix"
    assert _read(target, 6, target_offset - 6) == b"\x00" * (target_offset - 6)
    assert _read(target, target_offset, len(data)) == data

    short_source = os.path.join(test_dir, "short-source")
    short_target = os.path.join(test_dir, "short-target")
    short_data = _data(101, 6403)
    _write(short_source, short_data)
    open(short_target, "wb").close()
    source_fd = os.open(short_source, os.O_RDONLY)
    target_fd = os.open(short_target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, 1000, 17, 23) == len(short_data) - 17
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert _read(short_target) == b"\x00" * 23 + short_data[17:]


def test_copy_file_range_zero_length_eof_and_sparse_source(test_dir):
    source = os.path.join(test_dir, "source")
    target = os.path.join(test_dir, "target")
    data = _data(_BLOCK, 6404)
    _write(source, data)
    _write(target, b"unchanged")
    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, 0, 0, 0) == 0
        assert _copy(source_fd, target_fd, 1, len(data), 0) == 0
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert _read(target) == b"unchanged"

    empty = os.path.join(test_dir, "empty-source")
    empty_target = os.path.join(test_dir, "empty-target")
    open(empty, "wb").close()
    _write(empty_target, b"keep")
    source_fd = os.open(empty, os.O_RDONLY)
    target_fd = os.open(empty_target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, _BLOCK, 0, 9) == 0
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert _read(empty_target) == b"keep"

    sparse = os.path.join(test_dir, "sparse-source")
    sparse_target = os.path.join(test_dir, "sparse-target")
    fd = os.open(sparse, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 4 * _BLOCK)
        os.pwrite(fd, b"begin", 0)
        os.pwrite(fd, b"end", 4 * _BLOCK - 3)
    finally:
        os.close(fd)
    open(sparse_target, "wb").close()
    source_fd = os.open(sparse, os.O_RDONLY)
    target_fd = os.open(sparse_target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, 4 * _BLOCK, 0, 0) == 4 * _BLOCK
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert _read(sparse_target, 0, 5) == b"begin"
    assert _read(sparse_target, _BLOCK, 2 * _BLOCK) == b"\x00" * (2 * _BLOCK)
    assert _read(sparse_target, 4 * _BLOCK - 3, 3) == b"end"


def test_copy_file_range_same_file_and_descriptor_positions(test_dir):
    path = os.path.join(test_dir, "same")
    data = _data(10 * _BLOCK, 6405)
    _write(path, data)
    fd = os.open(path, os.O_RDWR)
    try:
        os.lseek(fd, 123, os.SEEK_SET)
        assert _copy(fd, fd, 2 * _BLOCK, 0, 5 * _BLOCK) == 2 * _BLOCK
        assert os.lseek(fd, 0, os.SEEK_CUR) == 123
    finally:
        os.close(fd)
    expected = data[:5 * _BLOCK] + data[:2 * _BLOCK] + data[7 * _BLOCK:]
    assert _read(path) == expected

    adjacent = os.path.join(test_dir, "adjacent")
    adjacent_data = _data(4 * _BLOCK, 6406)
    _write(adjacent, adjacent_data)
    fd = os.open(adjacent, os.O_RDWR)
    try:
        assert _copy(fd, fd, 2 * _BLOCK, 0, 2 * _BLOCK) == 2 * _BLOCK
    finally:
        os.close(fd)
    assert _read(adjacent) == adjacent_data[:2 * _BLOCK] * 2

    overlap = os.path.join(test_dir, "overlap")
    overlap_data = _data(6 * _BLOCK, 6407)
    _write(overlap, overlap_data)
    fd = os.open(overlap, os.O_RDWR)
    try:
        with pytest.raises(OSError) as exc:
            _copy(fd, fd, 2 * _BLOCK, 0, _BLOCK)
        assert exc.value.errno == errno.EINVAL
    finally:
        os.close(fd)
    assert _read(overlap) == overlap_data

    source = os.path.join(test_dir, "positions-source")
    target = os.path.join(test_dir, "positions-target")
    _write(source, _data(2 * _BLOCK, 6408))
    _write(target, b"x" * _BLOCK)
    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        os.lseek(source_fd, 7, os.SEEK_SET)
        os.lseek(target_fd, 11, os.SEEK_SET)
        assert _copy(source_fd, target_fd, 100) == 100
        assert os.lseek(source_fd, 0, os.SEEK_CUR) == 107
        assert os.lseek(target_fd, 0, os.SEEK_CUR) == 111
        with pytest.raises(OSError) as exc:
            _copy(source_fd, target_fd, 10, 0, 0, flags=1)
        assert exc.value.errno == errno.EINVAL
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert _read(target, 11, 100) == _read(source, 7, 100)


def test_copy_file_range_isolates_files_and_survives_source_unlink(test_dir):
    source = os.path.join(test_dir, "source")
    target = os.path.join(test_dir, "target")
    data = _data(3 * _BLOCK + 17, 6409)
    _write(source, data)
    open(target, "wb").close()
    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        assert _copy(source_fd, target_fd, len(data), 0, 0) == len(data)
    finally:
        os.close(source_fd)
        os.close(target_fd)

    fd = os.open(source, os.O_RDWR)
    try:
        os.pwrite(fd, b"SRC", _BLOCK + 10)
    finally:
        os.close(fd)
    assert _read(target) == data

    fd = os.open(target, os.O_RDWR)
    try:
        os.pwrite(fd, b"DST", 2 * _BLOCK + 10)
    finally:
        os.close(fd)
    expected_source = data[:_BLOCK + 10] + b"SRC" + data[_BLOCK + 13:]
    expected_target = data[:2 * _BLOCK + 10] + b"DST" + data[2 * _BLOCK + 13:]
    assert _read(source) == expected_source
    os.unlink(source)
    assert not os.path.exists(source)
    assert _read(target) == expected_target


def test_copy_file_range_rejects_invalid_descriptors(test_dir):
    source = os.path.join(test_dir, "source")
    target = os.path.join(test_dir, "target")
    _write(source, b"source")
    _write(target, b"target")

    target_fd = os.open(target, os.O_RDWR)
    try:
        with pytest.raises(OSError) as exc:
            _copy(-1, target_fd, 1, 0, 0)
        assert exc.value.errno == errno.EBADF
    finally:
        os.close(target_fd)

    source_fd = os.open(source, os.O_WRONLY)
    target_fd = os.open(target, os.O_WRONLY)
    try:
        with pytest.raises(OSError) as exc:
            _copy(source_fd, target_fd, 1, 0, 0)
        assert exc.value.errno == errno.EBADF
    finally:
        os.close(source_fd)
        os.close(target_fd)

    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDONLY)
    try:
        with pytest.raises(OSError) as exc:
            _copy(source_fd, target_fd, 1, 0, 0)
        assert exc.value.errno == errno.EBADF
    finally:
        os.close(source_fd)
        os.close(target_fd)

    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        for source_offset, target_offset in ((-1, 0), (0, -1)):
            with pytest.raises(OSError) as exc:
                _copy(source_fd, target_fd, 1, source_offset, target_offset)
            assert exc.value.errno in {errno.EINVAL, errno.EFBIG, errno.EOVERFLOW}
    finally:
        os.close(source_fd)
        os.close(target_fd)

    directory_fd = os.open(test_dir, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    try:
        for source_fd, destination_fd in ((directory_fd, target_fd),
                                          (target_fd, directory_fd)):
            with pytest.raises(OSError) as exc:
                _copy(source_fd, destination_fd, 1, 0, 0)
            assert exc.value.errno in {errno.EBADF, errno.EISDIR, errno.EINVAL}
    finally:
        os.close(directory_fd)
        os.close(target_fd)


@pytest.mark.slow
def test_copy_file_range_across_chunks_and_in_parallel(test_dir):
    source = os.path.join(test_dir, "multi-source")
    target = os.path.join(test_dir, "multi-target")
    fd = os.open(source, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        size = 2 * _CHUNK + 512
        os.ftruncate(fd, size)
        for offset, data in ((0, b"start"), (_CHUNK - 8, b"before"),
                             (_CHUNK + 8, b"after"), (2 * _CHUNK + 400, b"last")):
            os.pwrite(fd, data, offset)
    finally:
        os.close(fd)
    open(target, "wb").close()

    source_fd = os.open(source, os.O_RDONLY)
    target_fd = os.open(target, os.O_RDWR)
    target_offset = _CHUNK - 128
    try:
        assert _copy(source_fd, target_fd, size, 0, target_offset) == size
    finally:
        os.close(source_fd)
        os.close(target_fd)
    assert os.path.getsize(target) == target_offset + size
    assert _read(target, target_offset, 5) == b"start"
    assert _read(target, target_offset + _CHUNK - 8, 6) == b"before"
    assert _read(target, target_offset + _CHUNK + 8, 5) == b"after"
    assert _read(target, target_offset + 2 * _CHUNK + 400, 4) == b"last"
    assert _read(target, target_offset + 100, 100) == b"\x00" * 100

    data = _data(256 * 1024 + 13, 6410)
    parallel_source = os.path.join(test_dir, "parallel-source")
    _write(parallel_source, data)
    errors = []
    lock = threading.Lock()

    def worker(index):
        path = os.path.join(test_dir, f"parallel-{index}")
        for attempt in range(10):
            source_fd = os.open(parallel_source, os.O_RDONLY)
            target_fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                if _copy(source_fd, target_fd, len(data), 0, 0) == len(data):
                    return
            except OSError as error:
                if error.errno == errno.EAGAIN and attempt < 9:
                    time.sleep(0.02)
                    continue
                message = str(error)
            else:
                message = "short copy"
            finally:
                os.close(source_fd)
                os.close(target_fd)
        with lock:
            errors.append((index, message))

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(
        int(os.environ.get("DINGOFS_CONC", "4"))
    )]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    for index in range(len(threads)):
        assert _read(os.path.join(test_dir, f"parallel-{index}")) == data
