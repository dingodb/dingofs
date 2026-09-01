"""Linux syscalls that Python's os module does not expose completely."""

import ctypes
import ctypes.util
import errno
import os

_LIBC = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
try:
    _COPY_FILE_RANGE = _LIBC.copy_file_range
except AttributeError:
    _COPY_FILE_RANGE = None
else:
    _COPY_FILE_RANGE.argtypes = [
        ctypes.c_int, ctypes.POINTER(ctypes.c_longlong), ctypes.c_int,
        ctypes.POINTER(ctypes.c_longlong), ctypes.c_size_t, ctypes.c_uint,
    ]
    _COPY_FILE_RANGE.restype = ctypes.c_ssize_t

FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02
FALLOC_FL_COLLAPSE_RANGE = 0x08
FALLOC_FL_ZERO_RANGE = 0x10
FALLOC_FL_INSERT_RANGE = 0x20


def fallocate(fd, mode, offset, length):
    """Run fallocate(2), returning false only for an unsupported operation."""
    result = _LIBC.fallocate(
        ctypes.c_int(fd),
        ctypes.c_int(mode),
        ctypes.c_longlong(offset),
        ctypes.c_longlong(length),
    )
    if result == 0:
        return True
    error = ctypes.get_errno()
    if error in {errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.ENODEV}:
        return False
    if mode and error == errno.EINVAL:
        return False
    raise OSError(error, os.strerror(error))


def copy_file_range(src_fd, dst_fd, count, src_offset=None, dst_offset=None,
                    flags=0):
    """Run copy_file_range(2), preserving descriptor positions when omitted."""
    if _COPY_FILE_RANGE is None:
        raise NotImplementedError("libc lacks copy_file_range(2)")
    source = ctypes.c_longlong(src_offset) if src_offset is not None else None
    destination = ctypes.c_longlong(dst_offset) if dst_offset is not None else None
    result = _COPY_FILE_RANGE(
        ctypes.c_int(src_fd),
        ctypes.byref(source) if source is not None else None,
        ctypes.c_int(dst_fd),
        ctypes.byref(destination) if destination is not None else None,
        ctypes.c_size_t(count),
        ctypes.c_uint(flags),
    )
    if result >= 0:
        return result
    error = ctypes.get_errno()
    raise OSError(error, os.strerror(error))
