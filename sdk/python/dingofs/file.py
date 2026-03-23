# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DingoFS file-like object."""

import errno

from .exceptions import DingofsError


def _raise_if_error(s) -> None:
    if not s.ok():
        raise DingofsError(s.ToSysErrNo(), s.ToString())


class DingoFile:
    """File-like object returned by :meth:`Client.open`.

    Supports the standard Python IO protocol: ``read``, ``write``,
    ``seek``, ``tell``, ``flush``, ``close``, and use as a context
    manager (``with`` statement).

    Do not instantiate directly — use :meth:`Client.open` instead.
    """

    def __init__(self, core, ino: int, fh: int, path: str, mode: str) -> None:
        self._core = core
        self._ino = ino
        self._fh = fh
        self._path = path
        self._mode = mode
        self._offset: int = 0
        self._closed: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """File path as passed to :meth:`Client.open`."""
        return self._path

    @property
    def mode(self) -> str:
        """Mode string as passed to :meth:`Client.open`."""
        return self._mode

    @property
    def closed(self) -> bool:
        """``True`` after :meth:`close` has been called."""
        return self._closed

    # ------------------------------------------------------------------
    # IO capability flags
    # ------------------------------------------------------------------

    def readable(self) -> bool:
        return "r" in self._mode or "+" in self._mode

    def writable(self) -> bool:
        return "w" in self._mode or "a" in self._mode or "+" in self._mode

    def seekable(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Core read / write
    # ------------------------------------------------------------------

    def read(self, size: int = -1) -> bytes:
        """Read up to *size* bytes; ``-1`` reads to end of file."""
        self._check_open()
        self._check_readable()
        if size == -1:
            s, attr = self._core.GetAttr(self._ino)
            _raise_if_error(s)
            size = max(0, attr.length - self._offset)
        if size == 0:
            return b""
        s, data = self._core.read_bytes(self._ino, size, self._offset, self._fh)
        _raise_if_error(s)
        self._offset += len(data)
        return data

    def read_into(self, buf: "bytearray | memoryview", size: int = -1) -> int:
        """Read directly into a writable buffer without an intermediate copy.

        *buf* must implement the writable Buffer Protocol
        (``bytearray``, ``memoryview`` over writable memory,
        ``numpy.ndarray`` with ``dtype=uint8``, etc.).

        *size* defaults to ``len(buf)``; pass a smaller value to read fewer
        bytes than the buffer capacity.

        Returns the number of bytes actually read.  The file position is
        advanced by that amount.
        """
        self._check_open()
        self._check_readable()
        if size == -1:
            size = len(buf)
        if size == 0:
            return 0
        s, n = self._core.read_into(self._ino, buf, size, self._offset, self._fh)
        _raise_if_error(s)
        self._offset += n
        return n

    def write(self, data: "bytes | bytearray | memoryview") -> int:
        """Write *data* and return the number of bytes written.

        *data* can be any object that implements the Buffer Protocol
        (``bytes``, ``bytearray``, ``memoryview``, ``numpy.ndarray``, etc.).
        The underlying buffer pointer is passed directly to C++ without
        copying.
        """
        self._check_open()
        self._check_writable()
        if "a" in self._mode:
            s, attr = self._core.GetAttr(self._ino)
            _raise_if_error(s)
            self._offset = attr.length
        s, wsize = self._core.Write(
            self._ino, data, len(data), self._offset, self._fh
        )
        _raise_if_error(s)
        self._offset += wsize
        return wsize

    def seek(self, offset: int, whence: int = 0) -> int:
        """Set the file position.

        *whence* is one of ``os.SEEK_SET`` (0), ``os.SEEK_CUR`` (1),
        or ``os.SEEK_END`` (2).  Returns the new absolute position.
        """
        self._check_open()
        if whence == 0:  # SEEK_SET
            new_offset = offset
        elif whence == 1:  # SEEK_CUR
            new_offset = self._offset + offset
        elif whence == 2:  # SEEK_END
            s, attr = self._core.GetAttr(self._ino)
            _raise_if_error(s)
            new_offset = attr.length + offset
        else:
            raise OSError(errno.EINVAL, f"invalid whence value: {whence}")
        if new_offset < 0:
            raise OSError(errno.EINVAL, "negative seek position")
        self._offset = new_offset
        return self._offset

    def tell(self) -> int:
        """Return the current file position."""
        self._check_open()
        return self._offset

    # ------------------------------------------------------------------
    # Flush and close
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Flush write buffers to the server."""
        self._check_open()
        s = self._core.Flush(self._ino, self._fh)
        _raise_if_error(s)

    def fsync(self, datasync: bool = False) -> None:
        """Sync file data to durable storage.

        If *datasync* is ``True``, only data (not metadata) is synced
        (equivalent to ``fdatasync``).
        """
        self._check_open()
        s = self._core.Fsync(self._ino, int(datasync), self._fh)
        _raise_if_error(s)

    def close(self) -> None:
        """Flush and release the file handle.  Idempotent."""
        if self._closed:
            return
        self._closed = True  # mark first to prevent re-entry
        try:
            s = self._core.Flush(self._ino, self._fh)
            _raise_if_error(s)
        finally:
            self._core.Release(self._ino, self._fh)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "DingoFile":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<DingoFile {self._path!r} mode={self._mode!r} {state}>"

    # ------------------------------------------------------------------
    # Internal checks
    # ------------------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")

    def _check_readable(self) -> None:
        if not self.readable():
            raise OSError(errno.EBADF, "file not open for reading")

    def _check_writable(self) -> None:
        if not self.writable():
            raise OSError(errno.EBADF, "file not open for writing")
