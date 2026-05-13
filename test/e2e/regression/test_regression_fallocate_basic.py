# Copyright 2026 DingoDB. All rights reserved.

"""Basic fallocate(2) semantics: mode=0 / KEEP_SIZE / PUNCH_HOLE.

Bug:
  Several scenarios in dingofs MDS mode mis-handled fallocate edge cases:
    - mode=0 on a 0-byte / chunk-aligned file returned EIO because PreAlloc
      propagated GetChunk's ENOT_FOUND verbatim.
    - PUNCH_HOLE / ZERO_RANGE failed with "beyond slice num(0)" because the
      filesystem dispatcher did not pre-allocate slice IDs for those modes.

Verification:
  pre fix:  test_fallocate_basic / test_fallocate_extend_from_chunk_boundary
            FAIL with EIO on MDS; test_fallocate_punch_hole / KEEP_SIZE pass.
  post fix: all tests PASS on MDS / Local / Memory.

Cross-PR refs in individual test docstrings (e.g. PR #882) — see those for
per-case context. See also test_regression_fallocate_modes.py for the
PUNCH_HOLE / ZERO_RANGE mode-coverage cases keyed to PR #880.
"""

import os
import errno
import ctypes
import ctypes.util
import hashlib

import pytest

pytestmark = pytest.mark.standard

MB = 1024 * 1024

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def fallocate(fd, mode, offset, length):
    ret = libc.fallocate(fd, mode, ctypes.c_long(offset), ctypes.c_long(length))
    if ret != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def md5(data):
    return hashlib.md5(data).hexdigest()


def _try_fallocate(fd, mode, offset, length):
    """Call fallocate; skip test if ENOTSUP/EOPNOTSUPP."""
    try:
        fallocate(fd, mode, offset, length)
    except OSError as e:
        if e.errno in (errno.ENOTSUP, errno.EOPNOTSUPP):
            pytest.skip(f"fallocate not supported (errno={e.errno})")
        raise


def test_fallocate_basic(test_dir):
    """fallocate(0, 0, 10MB) should set file size to 10MB."""
    path = os.path.join(test_dir, "fa_basic")
    fd = os.open(path, os.O_CREAT | os.O_WRONLY)
    try:
        _try_fallocate(fd, 0, 0, 10 * MB)
    finally:
        os.close(fd)

    assert os.path.getsize(path) == 10 * MB


def test_fallocate_extend(test_dir):
    """Extend from 10MB to 20MB via fallocate."""
    path = os.path.join(test_dir, "fa_extend")
    # Create 10MB file
    with open(path, "wb") as f:
        f.write(os.urandom(10 * MB))
    assert os.path.getsize(path) == 10 * MB

    fd = os.open(path, os.O_WRONLY)
    try:
        _try_fallocate(fd, 0, 10 * MB, 10 * MB)
    finally:
        os.close(fd)

    assert os.path.getsize(path) == 20 * MB


def test_fallocate_keep_size(test_dir):
    """fallocate with KEEP_SIZE should not change file size."""
    path = os.path.join(test_dir, "fa_keepsize")
    with open(path, "wb") as f:
        f.write(os.urandom(20 * MB))
    assert os.path.getsize(path) == 20 * MB

    fd = os.open(path, os.O_WRONLY)
    try:
        _try_fallocate(fd, FALLOC_FL_KEEP_SIZE, 20 * MB, 10 * MB)
    finally:
        os.close(fd)

    assert os.path.getsize(path) == 20 * MB


def test_fallocate_punch_hole(test_dir):
    """Punch a 3MB hole at 5MB; reading [6MB, 7MB) should return all zeros."""
    path = os.path.join(test_dir, "fa_punch")
    data = os.urandom(10 * MB)
    with open(path, "wb") as f:
        f.write(data)

    fd = os.open(path, os.O_WRONLY)
    try:
        _try_fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        5 * MB, 3 * MB)
    finally:
        os.close(fd)

    # Read the punched region [6MB, 7MB)
    with open(path, "rb") as f:
        f.seek(6 * MB)
        chunk = f.read(1 * MB)

    assert chunk == b"\x00" * (1 * MB), "punched region should be all zeros"


def test_fallocate_extend_from_chunk_boundary(test_dir):
    """fallocate(mode=0) starting exactly at chunk boundary (64MB) must succeed.

    Bug:
      When length is chunk-aligned, length/chunk_size points at a chunk index
      that hasn't been written yet — GetChunk returns ENOT_FOUND in PreAlloc.
      Pre-fix this propagated to the client as EIO.

    Verification:
      pre fix:  fallocate(mode=0, 64MB, 36MB) on a 64MB file → OSError EIO.
      post fix: extends file to 100MB and returns 0.

    Regression for PR #882: PreAlloc treats GetChunk's ENOT_FOUND as "no
    existing chunk to append to" and routes through the create-new-chunk
    branch (which properly initializes chunk_size / block_size).
    """
    path = os.path.join(test_dir, "fa_chunk_aligned")
    chunk_size = 64 * MB

    # Build a file that ends exactly at the chunk boundary
    with open(path, "wb") as f:
        f.write(b"x" * chunk_size)
    assert os.path.getsize(path) == chunk_size

    fd = os.open(path, os.O_WRONLY)
    try:
        _try_fallocate(fd, 0, chunk_size, 36 * MB)
    finally:
        os.close(fd)

    assert os.path.getsize(path) == chunk_size + 36 * MB


def test_fallocate_extend_across_chunk_boundary(test_dir):
    """fallocate(mode=0) starting mid-chunk and crossing into a new chunk.

    Bug / sanity:
      Exercises the loop branch where the first iteration appends a zero slice
      to the existing tail chunk (else branch) and subsequent iterations
      create a fresh chunk in the next index (if branch).

    Verification:
      pre fix:  this specific code path was not broken pre-fix; the test
                serves as a sanity check that the post-fix state machine
                (max_chunk_exists reset after the else branch) still handles
                the else→if transition correctly.
      post fix: file extends to 2*chunk_size + 2MB.

    Regression for PR #882: ensures the post-fix max_chunk_exists state
    machine doesn't accidentally regress this multi-chunk transition.
    """
    path = os.path.join(test_dir, "fa_cross_boundary")
    chunk_size = 64 * MB

    # File ends mid-chunk-1 at 65 MB
    with open(path, "wb") as f:
        f.write(b"y" * (chunk_size + MB))
    assert os.path.getsize(path) == chunk_size + MB

    # Extend by 65 MB — crosses into chunk 2 (last 2 MB land in chunk 2)
    fd = os.open(path, os.O_WRONLY)
    try:
        _try_fallocate(fd, 0, chunk_size + MB, 65 * MB)
    finally:
        os.close(fd)

    assert os.path.getsize(path) == 2 * chunk_size + 2 * MB


def test_fallocate_not_supported_skip(test_dir):
    """If fallocate returns ENOTSUP, the test should be skipped."""
    path = os.path.join(test_dir, "fa_notsup")
    fd = os.open(path, os.O_CREAT | os.O_WRONLY)
    try:
        _try_fallocate(fd, 0, 0, 1 * MB)
    finally:
        os.close(fd)
    # If we get here, fallocate is supported -- that is fine, test passes.
