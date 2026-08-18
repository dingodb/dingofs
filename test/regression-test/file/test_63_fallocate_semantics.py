#!/usr/bin/env python3
"""Case 63: fallocate(2) semantics and boundary coverage.

Covers plain allocation, KEEP_SIZE, PUNCH_HOLE, ZERO_RANGE, file-position and
reopen visibility, chunk boundaries, invalid arguments, and fd errors.
Unsupported fallocate modes are skipped so the case also runs on backends that
only implement the basic operation.
"""
import errno
import os

from common import (FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE,
                    FALLOC_FL_ZERO_RANGE, fallocate, rand_data, run_case)

BLOCK = 4096
CHUNK = 64 << 20


def read_range(path, offset, length):
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(length)


def do_fallocate(c, fd, mode, offset, length, label):
    """Call fallocate and skip only a mode unsupported by this filesystem."""
    if fallocate(fd, mode, offset, length):
        c.check(True, label)
        return True
    print("  [SKIP] %s (mode unsupported)" % label)
    return False


def plain_allocate(c, d):
    p = os.path.join(d, "plain")
    fd = os.open(p, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        if not do_fallocate(c, fd, 0, 0, BLOCK, "plain allocation on empty file"):
            c.check(True, "fallocate mode=0 unsupported; semantic checks skipped")
            return False
        c.check_eq(os.fstat(fd).st_size, BLOCK, "plain allocation changes file size")
        c.check_eq(os.pread(fd, BLOCK, 0), b"\x00" * BLOCK,
                   "plain allocation reads as zeros")

        os.lseek(fd, 123, os.SEEK_SET)
        do_fallocate(c, fd, 0, 0, BLOCK, "allocation inside existing file")
        c.check_eq(os.lseek(fd, 0, os.SEEK_CUR), 123,
                   "fallocate does not change file position")
        c.check_eq(os.fstat(fd).st_size, BLOCK,
                   "allocation within EOF does not change size")
    finally:
        os.close(fd)

    p = os.path.join(d, "plain_extend")
    prefix = rand_data(BLOCK, seed=6301)
    with open(p, "wb") as f:
        f.write(prefix)
    old_size = len(prefix)
    fd = os.open(p, os.O_RDWR)
    try:
        offset = old_size + BLOCK
        length = 2 * BLOCK
        do_fallocate(c, fd, 0, offset, length,
                     "plain allocation with nonzero offset")
    finally:
        os.close(fd)
    c.check_eq(os.path.getsize(p), offset + length,
               "plain allocation extends to offset plus length")
    c.check_eq(read_range(p, 0, old_size), prefix,
               "plain allocation preserves existing data")
    c.check_eq(read_range(p, old_size, BLOCK), b"\x00" * BLOCK,
               "gap before plain allocation reads as zeros")
    c.check_eq(read_range(p, offset, length), b"\x00" * length,
               "plain allocated range reads as zeros")
    return True


def keep_size_and_zeroing(c, d):
    p = os.path.join(d, "modes")
    size = 8 * BLOCK
    data = bytes((i // BLOCK) % 4 + 65 for i in range(size))
    with open(p, "wb") as f:
        f.write(data)

    fd = os.open(p, os.O_RDWR)
    try:
        if not do_fallocate(c, fd, FALLOC_FL_KEEP_SIZE, size + BLOCK, BLOCK,
                            "KEEP_SIZE beyond EOF"):
            return
        c.check_eq(os.fstat(fd).st_size, size,
                   "KEEP_SIZE does not extend file size")
        c.check_eq(os.pread(fd, size, 0), data,
                   "KEEP_SIZE beyond EOF preserves file data")

        tail_off = size - BLOCK // 2
        expected = data
        if do_fallocate(c, fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        tail_off, 2 * BLOCK, "PUNCH_HOLE past EOF"):
            expected = data[:tail_off] + b"\x00" * (size - tail_off)
            c.check_eq(os.fstat(fd).st_size, size,
                       "PUNCH_HOLE past EOF preserves size")
            c.check_eq(read_range(p, tail_off, size - tail_off),
                       b"\x00" * (size - tail_off),
                       "PUNCH_HOLE past EOF zeros only through EOF")

        punch_off = BLOCK + 123
        punch_len = 2 * BLOCK + 257
        if do_fallocate(c, fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        punch_off, punch_len, "PUNCH_HOLE with KEEP_SIZE"):
            c.check_eq(os.fstat(fd).st_size, size,
                       "PUNCH_HOLE preserves file size")
            c.check_eq(read_range(p, 0, punch_off), data[:punch_off],
                       "PUNCH_HOLE preserves prefix")
            c.check_eq(read_range(p, punch_off, punch_len),
                       b"\x00" * punch_len, "PUNCH_HOLE zeros requested range")
            c.check_eq(read_range(p, punch_off + punch_len,
                                  size - punch_off - punch_len),
                       expected[punch_off + punch_len:],
                       "PUNCH_HOLE preserves suffix")

        zero_off = 5 * BLOCK + 37
        zero_len = BLOCK + 101
        if do_fallocate(c, fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
                        zero_off, zero_len, "ZERO_RANGE with KEEP_SIZE"):
            c.check_eq(os.fstat(fd).st_size, size,
                       "ZERO_RANGE|KEEP_SIZE preserves file size")
            c.check_eq(read_range(p, zero_off, zero_len), b"\x00" * zero_len,
                       "ZERO_RANGE|KEEP_SIZE zeros requested range")
    finally:
        os.close(fd)

    p = os.path.join(d, "zero_extend")
    prefix = rand_data(2 * BLOCK, seed=6302)
    with open(p, "wb") as f:
        f.write(prefix)
    offset = 3 * BLOCK + 17
    length = 2 * BLOCK + 29
    fd = os.open(p, os.O_RDWR)
    try:
        if not do_fallocate(c, fd, FALLOC_FL_ZERO_RANGE, offset, length,
                            "ZERO_RANGE beyond EOF"):
            return
    finally:
        os.close(fd)
    c.check_eq(os.path.getsize(p), offset + length,
               "ZERO_RANGE without KEEP_SIZE extends file")
    c.check_eq(read_range(p, 0, len(prefix)), prefix,
               "ZERO_RANGE extension preserves prefix")
    c.check_eq(read_range(p, len(prefix), offset + length - len(prefix)),
               b"\x00" * (offset + length - len(prefix)),
               "ZERO_RANGE extension reads as zeros")


def chunk_boundary(c, d):
    p = os.path.join(d, "chunk_boundary")
    with open(p, "wb") as f:
        f.truncate(CHUNK)
    fd = os.open(p, os.O_RDWR)
    try:
        if not do_fallocate(c, fd, 0, CHUNK, 2 * BLOCK,
                            "plain allocation at chunk boundary"):
            return
    finally:
        os.close(fd)
    c.check_eq(os.path.getsize(p), CHUNK + 2 * BLOCK,
               "allocation at chunk boundary extends file")
    c.check_eq(read_range(p, CHUNK, 2 * BLOCK), b"\x00" * (2 * BLOCK),
               "chunk-boundary extension reads as zeros")


def no_op_and_errors(c, d):
    p = os.path.join(d, "errors")
    original = rand_data(BLOCK, seed=6303)
    with open(p, "wb") as f:
        f.write(original)

    fd = os.open(p, os.O_RDWR)
    try:
        size = os.fstat(fd).st_size
        for mode, name in ((0, "plain"),
                           (FALLOC_FL_KEEP_SIZE, "KEEP_SIZE"),
                           (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                            "PUNCH_HOLE"),
                           (FALLOC_FL_ZERO_RANGE, "ZERO_RANGE")):
            try:
                supported = fallocate(fd, mode, BLOCK // 2, 0)
            except OSError as e:
                c.check_eq(e.errno, errno.EINVAL,
                           "zero-length %s is rejected with EINVAL" % name)
                supported = False
            if supported:
                c.check(True, "zero-length %s is a no-op" % name)
        c.check_eq(os.fstat(fd).st_size, size,
                   "zero-length operations preserve size")
        c.check_eq(os.pread(fd, BLOCK, 0), original,
                   "zero-length operations preserve data")

        c.check(not fallocate(fd, FALLOC_FL_PUNCH_HOLE, 0, BLOCK),
                "PUNCH_HOLE without KEEP_SIZE is rejected")
        c.check(not fallocate(fd, 0x04, 0, BLOCK),
                "unknown fallocate flag is rejected")
        c.check_raises({errno.EINVAL},
                       lambda: fallocate(fd, 0, -1, BLOCK),
                       "negative offset is rejected")
        c.check_raises({errno.EINVAL},
                       lambda: fallocate(fd, 0, 0, -1),
                       "negative length is rejected")
    finally:
        os.close(fd)

    read_only = os.open(p, os.O_RDONLY)
    try:
        c.check_raises({errno.EBADF},
                       lambda: fallocate(read_only, 0, 0, BLOCK),
                       "allocation on read-only fd is rejected")
    finally:
        os.close(read_only)

    dir_fd = os.open(d, os.O_RDONLY)
    try:
        c.check_raises({errno.EBADF, errno.EINVAL, errno.EISDIR},
                       lambda: fallocate(dir_fd, 0, 0, BLOCK),
                       "allocation on directory fd is rejected")
    finally:
        os.close(dir_fd)


def reopen_visibility(c, d):
    p = os.path.join(d, "reopen")
    with open(p, "wb") as f:
        f.write(b"A" * (4 * BLOCK))

    fd = os.open(p, os.O_RDWR)
    try:
        if not do_fallocate(c, fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
                            BLOCK, BLOCK, "zero range on open fd"):
            return
        c.check_eq(os.pread(fd, BLOCK, BLOCK), b"\x00" * BLOCK,
                   "same fd observes zero range")
        os.fsync(fd)
    finally:
        os.close(fd)
    c.check_eq(read_range(p, BLOCK, BLOCK), b"\x00" * BLOCK,
               "reopened fd observes zero range")


def case(c, d):
    if not plain_allocate(c, d):
        return
    keep_size_and_zeroing(c, d)
    chunk_boundary(c, d)
    no_op_and_errors(c, d)
    reopen_visibility(c, d)


if __name__ == "__main__":
    run_case("63_fallocate_semantics", case)
