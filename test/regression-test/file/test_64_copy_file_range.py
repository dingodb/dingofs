#!/usr/bin/env python3
"""Case 64: copy_file_range(2) semantics and boundary coverage.

Covers cross-file copies, offsets, EOF truncation, sparse files, destination
extension, same-file copies and overlap rejection, flags, fd positions,
permissions, invalid fds, chunk boundaries, copy-on-write isolation, source
unlinking, reopen visibility, and concurrent independent copies.
"""
import ctypes
import ctypes.util
import errno
import os
import threading
import time

from common import rand_data, run_case


BLOCK = 4096
CHUNK = 64 << 20


def _load_copy_file_range():
    """Load libc copy_file_range because this Python build may omit os' wrapper."""
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        fn = libc.copy_file_range
    except (AttributeError, OSError):
        return None
    fn.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_longlong),
                   ctypes.c_int, ctypes.POINTER(ctypes.c_longlong),
                   ctypes.c_size_t, ctypes.c_uint]
    fn.restype = ctypes.c_ssize_t
    return fn


_COPY_FILE_RANGE = _load_copy_file_range()


def copy_file_range(src_fd, dst_fd, count, src_offset=None, dst_offset=None,
                    flags=0):
    """Call copy_file_range(2), preserving offsets when an offset is supplied."""
    src = ctypes.c_longlong(src_offset) if src_offset is not None else None
    dst = ctypes.c_longlong(dst_offset) if dst_offset is not None else None
    result = _COPY_FILE_RANGE(
        src_fd, ctypes.byref(src) if src is not None else None,
        dst_fd, ctypes.byref(dst) if dst is not None else None,
        count, flags)
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return int(result)


def read_at(fd, offset, length):
    return os.pread(fd, length, offset)


def write_file(path, data):
    with open(path, "wb") as f:
        f.write(data)


def basic_and_offsets(c, d):
    src = os.path.join(d, "basic_src")
    dst = os.path.join(d, "basic_dst")
    data = rand_data(5 * BLOCK + 37, seed=6401)
    write_file(src, data)
    open(dst, "wb").close()

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    try:
        n = copy_file_range(src_fd, dst_fd, len(data), 0, 0)
        c.check_eq(n, len(data), "basic copy returns the requested byte count")
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    c.check_eq(open(dst, "rb").read(), data,
               "basic cross-file copy preserves all bytes")
    c.check_eq(os.path.getsize(dst), len(data),
               "basic copy sets destination size")

    # Copy a middle range into an existing destination without changing its
    # prefix or suffix.
    dst_data = b"D" * (6 * BLOCK)
    write_file(dst, dst_data)
    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    src_off = BLOCK + 11
    dst_off = 2 * BLOCK + 19
    count = 3 * BLOCK + 23
    try:
        n = copy_file_range(src_fd, dst_fd, count, src_off, dst_off)
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    expected = (dst_data[:dst_off] + data[src_off:src_off + count] +
                dst_data[dst_off + count:])
    c.check_eq(n, count, "offset copy returns the exact requested count")
    c.check_eq(open(dst, "rb").read(), expected,
               "offset copy preserves destination prefix and suffix")
    c.check_eq(os.path.getsize(dst), len(dst_data),
               "copy inside destination does not change its size")


def destination_extension_and_eof(c, d):
    src = os.path.join(d, "extend_src")
    dst = os.path.join(d, "extend_dst")
    data = rand_data(2 * BLOCK + 31, seed=6402)
    write_file(src, data)
    write_file(dst, b"prefix")

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    dst_off = 3 * BLOCK + 7
    try:
        n = copy_file_range(src_fd, dst_fd, len(data), 0, dst_off)
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    expected_size = dst_off + len(data)
    c.check_eq(n, len(data), "copy beyond EOF returns all available source bytes")
    c.check_eq(os.path.getsize(dst), expected_size,
               "copy beyond EOF extends destination")
    fd = os.open(dst, os.O_RDONLY)
    try:
        c.check_eq(read_at(fd, 5, 1), b"x",
                   "destination prefix is preserved")
    finally:
        os.close(fd)
    fd = os.open(dst, os.O_RDONLY)
    try:
        c.check_eq(read_at(fd, 6, dst_off - 6), b"\x00" * (dst_off - 6),
                   "destination gap reads as zeros")
        c.check_eq(read_at(fd, dst_off, len(data)), data,
                   "extended destination contains copied data")
    finally:
        os.close(fd)

    # The returned count and destination size must be clamped at source EOF.
    short_src = os.path.join(d, "short_src")
    short_dst = os.path.join(d, "short_dst")
    short_data = rand_data(101, seed=6403)
    write_file(short_src, short_data)
    open(short_dst, "wb").close()
    src_fd = os.open(short_src, os.O_RDONLY)
    dst_fd = os.open(short_dst, os.O_RDWR)
    try:
        n = copy_file_range(src_fd, dst_fd, 1000, 17, 23)
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    c.check_eq(n, len(short_data) - 17,
               "copy count is clamped at source EOF")
    c.check_eq(os.path.getsize(short_dst), 23 + len(short_data) - 17,
               "destination size uses the clamped count")
    c.check_eq(open(short_dst, "rb").read(),
               b"\x00" * 23 + short_data[17:],
               "clamped copy contains only the remaining source bytes")


def zero_empty_and_sparse(c, d):
    src = os.path.join(d, "zero_src")
    dst = os.path.join(d, "zero_dst")
    data = rand_data(BLOCK, seed=6404)
    write_file(src, data)
    write_file(dst, b"unchanged")

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    try:
        c.check_eq(copy_file_range(src_fd, dst_fd, 0, 0, 0), 0,
                   "zero-length copy returns zero")
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    c.check_eq(open(dst, "rb").read(), b"unchanged",
               "zero-length copy does not modify destination")

    empty = os.path.join(d, "empty_src")
    empty_dst = os.path.join(d, "empty_dst")
    open(empty, "wb").close()
    write_file(empty_dst, b"keep")
    src_fd = os.open(empty, os.O_RDONLY)
    dst_fd = os.open(empty_dst, os.O_RDWR)
    try:
        c.check_eq(copy_file_range(src_fd, dst_fd, BLOCK, 0, 9), 0,
                   "copy from an empty source returns zero")
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    c.check_eq(open(empty_dst, "rb").read(), b"keep",
               "empty-source copy does not change destination")

    eof_dst = os.path.join(d, "eof_dst")
    write_file(eof_dst, b"keep")
    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(eof_dst, os.O_RDWR)
    try:
        c.check_eq(copy_file_range(src_fd, dst_fd, 1, len(data), 0), 0,
                   "copy starting exactly at source EOF returns zero")
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    c.check_eq(open(eof_dst, "rb").read(), b"keep",
               "copy at source EOF leaves destination unchanged")

    # Only write two small ranges in a multi-block sparse source.  The hole is
    # part of the copied range and must read back as zeroes.
    sparse = os.path.join(d, "sparse_src")
    sparse_dst = os.path.join(d, "sparse_dst")
    fd = os.open(sparse, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 4 * BLOCK)
        os.pwrite(fd, b"begin", 0)
        os.pwrite(fd, b"end", 4 * BLOCK - 3)
    finally:
        os.close(fd)
    open(sparse_dst, "wb").close()
    src_fd = os.open(sparse, os.O_RDONLY)
    dst_fd = os.open(sparse_dst, os.O_RDWR)
    try:
        n = copy_file_range(src_fd, dst_fd, 4 * BLOCK, 0, 0)
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    fd = os.open(sparse_dst, os.O_RDONLY)
    try:
        c.check_eq(n, 4 * BLOCK, "sparse source copy returns the full range")
        c.check_eq(read_at(fd, 0, 5), b"begin", "sparse source prefix is copied")
        c.check_eq(read_at(fd, BLOCK, 2 * BLOCK), b"\x00" * (2 * BLOCK),
                   "sparse source hole remains zero")
        c.check_eq(read_at(fd, 4 * BLOCK - 3, 3), b"end",
                   "sparse source suffix is copied")
    finally:
        os.close(fd)


def same_file_ranges(c, d):
    p = os.path.join(d, "same_file")
    original = rand_data(10 * BLOCK, seed=6405)
    write_file(p, original)

    fd = os.open(p, os.O_RDWR)
    try:
        os.lseek(fd, 123, os.SEEK_SET)
        n = copy_file_range(fd, fd, 2 * BLOCK, 0, 5 * BLOCK)
        expected = original[:5 * BLOCK] + original[:2 * BLOCK] + original[7 * BLOCK:]
        c.check_eq(n, 2 * BLOCK, "same-file non-overlap copy succeeds")
        c.check_eq(os.lseek(fd, 0, os.SEEK_CUR), 123,
                   "explicit offsets do not change file position")
        c.check_eq(os.fstat(fd).st_size, len(original),
                   "same-file copy does not change file size")
        c.check_eq(read_at(fd, 0, len(original)), expected,
                   "same-file non-overlap copy has memmove-like result")
    finally:
        os.close(fd)

    # Adjacent ranges are not overlapping and must be accepted.
    adjacent = os.path.join(d, "adjacent")
    data = rand_data(4 * BLOCK, seed=6406)
    write_file(adjacent, data)
    fd = os.open(adjacent, os.O_RDWR)
    try:
        c.check_eq(copy_file_range(fd, fd, 2 * BLOCK, 0, 2 * BLOCK), 2 * BLOCK,
                   "same-file adjacent ranges are allowed")
    finally:
        os.close(fd)
    c.check_eq(open(adjacent, "rb").read(), data[:2 * BLOCK] * 2,
               "adjacent same-file copy has the expected result")

    overlap = os.path.join(d, "overlap")
    overlap_data = rand_data(6 * BLOCK, seed=6407)
    write_file(overlap, overlap_data)
    fd = os.open(overlap, os.O_RDWR)
    try:
        c.check_raises({errno.EINVAL},
                       lambda: copy_file_range(fd, fd, 2 * BLOCK, 0, BLOCK),
                       "overlapping same-file ranges return EINVAL")
    finally:
        os.close(fd)
    c.check_eq(open(overlap, "rb").read(), overlap_data,
               "rejected overlap leaves source data unchanged")


def flags_positions_and_reopen(c, d):
    src = os.path.join(d, "positions_src")
    dst = os.path.join(d, "positions_dst")
    data = rand_data(2 * BLOCK, seed=6408)
    write_file(src, data)
    write_file(dst, b"x" * BLOCK)

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    try:
        os.lseek(src_fd, 7, os.SEEK_SET)
        os.lseek(dst_fd, 11, os.SEEK_SET)
        n = copy_file_range(src_fd, dst_fd, 100, None, None)
        c.check_eq(n, 100, "copy with null offsets returns the requested count")
        c.check_eq(os.lseek(src_fd, 0, os.SEEK_CUR), 107,
                   "null source offset advances source fd position")
        c.check_eq(os.lseek(dst_fd, 0, os.SEEK_CUR), 111,
                   "null destination offset advances destination fd position")
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    fd = os.open(dst, os.O_RDONLY)
    try:
        c.check_eq(read_at(fd, 11, 100), data[7:107],
                   "null-offset copy uses the current fd positions")
    finally:
        os.close(fd)

    # Reopen visibility also catches implementations that only update a local
    # chunk cache and fail to commit the destination inode.
    reopened = os.open(dst, os.O_RDONLY)
    try:
        c.check_eq(read_at(reopened, 11, 100), data[7:107],
                   "copied data is visible after reopening")
    finally:
        os.close(reopened)

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    try:
        c.check_raises({errno.EINVAL},
                       lambda: copy_file_range(src_fd, dst_fd, 10, 0, 0, 1),
                       "nonzero flags return EINVAL")
    finally:
        os.close(src_fd)
        os.close(dst_fd)


def copy_on_write_and_unlink(c, d):
    src = os.path.join(d, "cow_src")
    dst = os.path.join(d, "cow_dst")
    data = rand_data(3 * BLOCK + 17, seed=6409)
    write_file(src, data)
    open(dst, "wb").close()
    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    try:
        c.check_eq(copy_file_range(src_fd, dst_fd, len(data), 0, 0), len(data),
                   "copy-on-write setup succeeds")
    finally:
        os.close(src_fd)
        os.close(dst_fd)

    src_fd = os.open(src, os.O_RDWR)
    try:
        os.pwrite(src_fd, b"SRC", BLOCK + 10)
    finally:
        os.close(src_fd)
    expected_dst = data
    c.check_eq(open(dst, "rb").read(), expected_dst,
               "source rewrite is not visible in destination")

    dst_fd = os.open(dst, os.O_RDWR)
    try:
        os.pwrite(dst_fd, b"DST", 2 * BLOCK + 10)
    finally:
        os.close(dst_fd)
    expected_src = data[:BLOCK + 10] + b"SRC" + data[BLOCK + 13:]
    c.check_eq(open(src, "rb").read(), expected_src,
               "destination rewrite is not visible in source")

    expected_dst_after_write = data[:2 * BLOCK + 10] + b"DST" + data[2 * BLOCK + 13:]
    os.unlink(src)
    c.check(not os.path.exists(src), "source path can be unlinked after copy")
    c.check_eq(open(dst, "rb").read(), expected_dst_after_write,
               "destination remains readable after source unlink")


def invalid_fds_and_file_types(c, d):
    src = os.path.join(d, "errors_src")
    dst = os.path.join(d, "errors_dst")
    write_file(src, b"source")
    write_file(dst, b"target")

    dst_fd = os.open(dst, os.O_RDWR)
    try:
        c.check_raises({errno.EBADF},
                       lambda: copy_file_range(-1, dst_fd, 1, 0, 0),
                       "invalid source fd returns EBADF")
    finally:
        os.close(dst_fd)

    src_write = os.open(src, os.O_WRONLY)
    dst_write = os.open(dst, os.O_WRONLY)
    try:
        c.check_raises({errno.EBADF},
                       lambda: copy_file_range(src_write, dst_write, 1, 0, 0),
                       "write-only source fd returns EBADF")
    finally:
        os.close(src_write)
        os.close(dst_write)

    src_read = os.open(src, os.O_RDONLY)
    dst_read = os.open(dst, os.O_RDONLY)
    try:
        c.check_raises({errno.EBADF},
                       lambda: copy_file_range(src_read, dst_read, 1, 0, 0),
                       "read-only destination fd returns EBADF")
    finally:
        os.close(src_read)
        os.close(dst_read)

    dir_fd = os.open(d, os.O_RDONLY)
    file_fd = os.open(dst, os.O_RDWR)
    try:
        c.check_raises({errno.EBADF, errno.EISDIR, errno.EINVAL},
                       lambda: copy_file_range(dir_fd, file_fd, 1, 0, 0),
                       "directory source fd is rejected")
        c.check_raises({errno.EBADF, errno.EISDIR, errno.EINVAL},
                       lambda: copy_file_range(file_fd, dir_fd, 1, 0, 0),
                       "directory destination fd is rejected")
    finally:
        os.close(dir_fd)
        os.close(file_fd)

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    try:
        c.check_raises({errno.EINVAL, errno.EFBIG, errno.EOVERFLOW},
                       lambda: copy_file_range(src_fd, dst_fd, 1, -1, 0),
                       "negative source offset is rejected")
        c.check_raises({errno.EINVAL, errno.EFBIG, errno.EOVERFLOW},
                       lambda: copy_file_range(src_fd, dst_fd, 1, 0, -1),
                       "negative destination offset is rejected")
    finally:
        os.close(src_fd)
        os.close(dst_fd)


def multi_chunk_and_concurrent(c, d):
    src = os.path.join(d, "multi_chunk_src")
    dst = os.path.join(d, "multi_chunk_dst")
    src_fd = os.open(src, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        source_size = 2 * CHUNK + 512
        os.ftruncate(src_fd, source_size)
        os.pwrite(src_fd, b"start", 0)
        os.pwrite(src_fd, b"before", CHUNK - 8)
        os.pwrite(src_fd, b"after", CHUNK + 8)
        os.pwrite(src_fd, b"last", 2 * CHUNK + 400)
    finally:
        os.close(src_fd)
    open(dst, "wb").close()

    src_fd = os.open(src, os.O_RDONLY)
    dst_fd = os.open(dst, os.O_RDWR)
    dst_off = CHUNK - 128
    count = 2 * CHUNK + 512
    try:
        n = copy_file_range(src_fd, dst_fd, count, 0, dst_off)
    finally:
        os.close(src_fd)
        os.close(dst_fd)
    c.check_eq(n, count, "multi-chunk copy returns the complete range")
    c.check_eq(os.path.getsize(dst), dst_off + count,
               "multi-chunk copy sets the final destination size")
    fd = os.open(dst, os.O_RDONLY)
    try:
        c.check_eq(read_at(fd, dst_off, 5), b"start",
                   "multi-chunk copy preserves first chunk data")
        c.check_eq(read_at(fd, dst_off + CHUNK - 8, 6), b"before",
                   "multi-chunk copy preserves data before chunk boundary")
        c.check_eq(read_at(fd, dst_off + CHUNK + 8, 5), b"after",
                   "multi-chunk copy preserves data after chunk boundary")
        c.check_eq(read_at(fd, dst_off + 2 * CHUNK + 400, 4), b"last",
                   "multi-chunk copy preserves final chunk data")
        c.check_eq(read_at(fd, dst_off + 100, 100), b"\x00" * 100,
                   "multi-chunk sparse gap reads as zeros")
    finally:
        os.close(fd)

    # Independent destinations should be able to copy the same source in
    # parallel. Retry only SI conflicts; other errors are real failures.
    source = os.path.join(d, "parallel_src")
    parallel_data = rand_data(256 * 1024 + 13, seed=6410)
    write_file(source, parallel_data)
    workers = int(os.environ.get("DINGOFS_CONC", "4"))
    errors = []
    lock = threading.Lock()

    def worker(index):
        target = os.path.join(d, "parallel_dst_%d" % index)
        for attempt in range(10):
            src_fd = os.open(source, os.O_RDONLY)
            dst_fd = os.open(target, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                n = copy_file_range(src_fd, dst_fd, len(parallel_data), 0, 0)
                if n == len(parallel_data):
                    return
                error = "short copy: %d" % n
            except OSError as e:
                if e.errno == errno.EAGAIN and attempt < 9:
                    time.sleep(0.02)
                    continue
                error = "errno=%d (%s)" % (e.errno, os.strerror(e.errno))
            finally:
                os.close(src_fd)
                os.close(dst_fd)
        with lock:
            errors.append((index, error))

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    c.check_eq(errors, [], "parallel independent copies have no errors")
    for i in range(workers):
        target = os.path.join(d, "parallel_dst_%d" % i)
        if os.path.exists(target):
            c.check_eq(open(target, "rb").read(), parallel_data,
                       "parallel destination %d is correct" % i)


def case(c, d):
    if _COPY_FILE_RANGE is None:
        print("  [SKIP] libc does not provide copy_file_range(2)")
        return
    basic_and_offsets(c, d)
    destination_extension_and_eof(c, d)
    zero_empty_and_sparse(c, d)
    same_file_ranges(c, d)
    flags_positions_and_reopen(c, d)
    copy_on_write_and_unlink(c, d)
    invalid_fds_and_file_types(c, d)
    multi_chunk_and_concurrent(c, d)


if __name__ == "__main__":
    run_case("64_copy_file_range", case)
