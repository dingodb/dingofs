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

"""DingoFS Python SDK client.

Wraps the low-level ``_dingofs_core`` extension (nanobind) with a
path-based helper that resolves paths to inode numbers via an internal
cache.  All public methods raise :class:`~dingofs.DingofsError` on
failure and return plain values on success — no ``(Status, value)``
tuples exposed to callers.

Typical usage::

    import dingofs

    client = dingofs.Client.build(
        mds_addrs="10.232.10.5:8801",
        fs_name="test03",
        mount_point="/mnt/test03",
    )

    client.mkdir("/data", 0o755)

    with client.open("/data/file.txt", "wb") as f:
        f.write(b"hello")

    client.stop()
"""

import os
from typing import List, Optional

try:
    from ._dingofs_core import (  # type: ignore[import]
        BindingClient,
        BindingConfig,
        Attr,
        DirEntry,
        FsStat,
        OptionInfo,
        SET_ATTR_MODE,
        SET_ATTR_UID,
        SET_ATTR_GID,
        SET_ATTR_SIZE,
        SET_ATTR_ATIME,
        SET_ATTR_MTIME,
        SET_ATTR_ATIME_NOW,
        SET_ATTR_MTIME_NOW,
        SET_ATTR_CTIME,
        SET_ATTR_FLAGS,
        set_option as _core_set_option,
        get_option as _core_get_option,
        list_options as _core_list_options,
        print_options as _core_print_options,
    )
except ModuleNotFoundError:
    BindingClient = None   # type: ignore[assignment,misc]
    BindingConfig = None   # type: ignore[assignment,misc]
    Attr = object          # type: ignore[assignment,misc]
    DirEntry = object      # type: ignore[assignment,misc]
    FsStat = object        # type: ignore[assignment,misc]
    OptionInfo = object    # type: ignore[assignment,misc]
    SET_ATTR_MODE      = 1 << 0  # type: ignore[assignment]
    SET_ATTR_UID       = 1 << 1  # type: ignore[assignment]
    SET_ATTR_GID       = 1 << 2  # type: ignore[assignment]
    SET_ATTR_SIZE      = 1 << 3  # type: ignore[assignment]
    SET_ATTR_ATIME     = 1 << 4  # type: ignore[assignment]
    SET_ATTR_MTIME     = 1 << 5  # type: ignore[assignment]
    SET_ATTR_ATIME_NOW = 1 << 6  # type: ignore[assignment]
    SET_ATTR_MTIME_NOW = 1 << 7  # type: ignore[assignment]
    SET_ATTR_CTIME     = 1 << 8  # type: ignore[assignment]
    SET_ATTR_FLAGS     = 1 << 9  # type: ignore[assignment]
    _core_set_option    = None   # type: ignore[assignment]
    _core_get_option    = None   # type: ignore[assignment]
    _core_list_options  = None   # type: ignore[assignment]
    _core_print_options = None   # type: ignore[assignment]

from .config import Config
from .exceptions import DingofsError
from .file import DingoFile

# Root inode number (kRootIno = 1 in meta.h)
_ROOT_INO: int = 1

# Mode string → os flags mapping
_MODE_MAP = {
    "r":    os.O_RDONLY,
    "rb":   os.O_RDONLY,
    "w":    os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
    "wb":   os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
    "a":    os.O_WRONLY | os.O_CREAT | os.O_APPEND,
    "ab":   os.O_WRONLY | os.O_CREAT | os.O_APPEND,
    "r+":   os.O_RDWR,
    "r+b":  os.O_RDWR,
    "rb+":  os.O_RDWR,
    "w+":   os.O_RDWR | os.O_CREAT | os.O_TRUNC,
    "w+b":  os.O_RDWR | os.O_CREAT | os.O_TRUNC,
    "wb+":  os.O_RDWR | os.O_CREAT | os.O_TRUNC,
    "a+":   os.O_RDWR | os.O_CREAT | os.O_APPEND,
    "a+b":  os.O_RDWR | os.O_CREAT | os.O_APPEND,
    "ab+":  os.O_RDWR | os.O_CREAT | os.O_APPEND,
}


def _raise_if_error(s) -> None:
    """Raise :class:`DingofsError` when *s* is not OK."""
    if not s.ok():
        raise DingofsError(s.ToSysErrNo(), s.ToString())


class ScandirIterator:
    """Lazy iterator returned by Client.scandir().

    Fetches directory entries from ReadDir in batches of :attr:`_BATCH_SIZE`
    rather than loading the whole directory into memory at once.

    * ``handler`` returns ``False`` after each batch → ReadDir stops early.
    * The ``offset`` carried by the last accepted entry is used as the
      starting point for the next ReadDir call (standard FUSE convention:
      offset passed to the handler = "resume here to get the next entry").
    * ``close()`` calls ReleaseDir immediately, even if iteration is not
      finished — matching the guarantee of ``os.scandir``.

    Supports both for-loop iteration and with-statement, like os.scandir().
    """

    _BATCH_SIZE: int = 64

    def __init__(self, core, ino: int, fh: int, path: str) -> None:
        self._core     = core
        self._ino      = ino
        self._fh       = fh
        self._buffer: list = []
        self._offset: int  = 0
        self._exhausted    = False
        self._closed       = False

    # ------------------------------------------------------------------
    # Iterator protocol
    # ------------------------------------------------------------------

    def __iter__(self):
        return self

    def __next__(self):
        if not self._buffer and not self._exhausted:
            self._fetch()
        if not self._buffer:
            raise StopIteration
        return self._buffer.pop(0)

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self) -> None:
        """Release the directory handle and discard any buffered entries."""
        if not self._closed:
            self._closed    = True
            self._exhausted = True
            self._buffer.clear()
            self._core.ReleaseDir(self._ino, self._fh)

    # ------------------------------------------------------------------
    # Internal: batch fetch
    # ------------------------------------------------------------------

    def _fetch(self) -> None:
        """Pull the next batch of entries via ReadDir."""
        if self._exhausted or self._closed:
            return

        batch: list = []
        last_offset  = self._offset

        def _handler(entry, offset: int) -> bool:
            nonlocal last_offset
            batch.append(entry)
            last_offset = offset
            return len(batch) < self._BATCH_SIZE

        s = self._core.ReadDir(self._ino, self._fh,
                               self._offset, True, _handler)
        _raise_if_error(s)

        self._buffer.extend(batch)

        if not batch or last_offset == self._offset:
            # No entries returned, or offset didn't advance → end of dir.
            self._exhausted = True
        else:
            self._offset = last_offset
            if len(batch) < self._BATCH_SIZE:
                # Received fewer entries than requested → last batch.
                self._exhausted = True


class Client:
    """Path-based wrapper around ``BindingClient``.

    All public methods raise :class:`~dingofs.DingofsError` on failure
    and return plain values on success.

    Use :meth:`build` (class method) to construct and start the client,
    or call :meth:`start` manually on a freshly created instance.
    """

    def __init__(self) -> None:
        self._core: Optional[BindingClient] = None

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def build(cls, mds_addrs: str, fs_name: str, mount_point: str,
              conf_file: str = "", log_dir: str = "",
              log_level: str = "", log_v: int = 0) -> "Client":
        """Create a ``Client``, start it, and return it.

        Raises:
            DingofsError: if mounting fails.
        """
        c = cls()
        cfg = Config(mds_addrs=mds_addrs, fs_name=fs_name,
                     mount_point=mount_point, conf_file=conf_file,
                     log_dir=log_dir, log_level=log_level, log_v=log_v)
        c.start(cfg)
        return c

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, config: Config) -> None:
        """Mount the filesystem and start background services.

        Raises:
            DingofsError: if mounting fails.
        """
        self._core = BindingClient()
        cfg = BindingConfig()
        cfg.mds_addrs   = config.mds_addrs
        cfg.fs_name     = config.fs_name
        cfg.mount_point = config.mount_point
        cfg.conf_file   = config.conf_file
        cfg.log_dir     = config.log_dir
        cfg.log_level   = config.log_level
        cfg.log_v       = config.log_v
        s = self._core.Start(cfg)
        _raise_if_error(s)

    def stop(self) -> None:
        """Unmount the filesystem and release all resources.

        Raises:
            DingofsError: if unmounting fails.
        """
        if self._core is None:
            return
        s = self._core.Stop()
        self._core = None
        _raise_if_error(s)

    # ------------------------------------------------------------------
    # Filesystem stats
    # ------------------------------------------------------------------

    def statfs(self) -> "FsStat":
        """Return filesystem-level usage statistics.

        Raises:
            DingofsError: on failure.
        """
        s, fsstat = self._core.StatFs(_ROOT_INO)
        _raise_if_error(s)
        return fsstat

    # ------------------------------------------------------------------
    # Inode attributes
    # ------------------------------------------------------------------

    def stat(self, path: str) -> "Attr":
        """Return inode attributes for *path*.

        Raises:
            DingofsError: if *path* does not exist or on IO error.
        """
        ino = self._lookup(path)
        s, attr = self._core.GetAttr(ino)
        _raise_if_error(s)
        return attr

    def set_attr(self, path: str, mask: int, attr: "Attr") -> "Attr":
        """Set inode attributes indicated by *mask*.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s, updated = self._core.SetAttr(ino, mask, attr)
        _raise_if_error(s)
        return updated

    def chmod(self, path: str, mode: int) -> "Attr":
        """Change permission bits of *path*.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        in_attr = Attr()
        in_attr.mode = mode
        s, updated = self._core.SetAttr(ino, SET_ATTR_MODE, in_attr)
        _raise_if_error(s)
        return updated

    def chown(self, path: str, uid: int, gid: int) -> "Attr":
        """Change owner/group of *path*.  Pass ``-1`` to leave unchanged.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        in_attr = Attr()
        mask = 0
        if uid != -1:
            in_attr.uid = uid
            mask |= SET_ATTR_UID
        if gid != -1:
            in_attr.gid = gid
            mask |= SET_ATTR_GID
        if mask == 0:
            s, attr = self._core.GetAttr(ino)
            _raise_if_error(s)
            return attr
        s, updated = self._core.SetAttr(ino, mask, in_attr)
        _raise_if_error(s)
        return updated

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def mkdir(self, path: str, mode: int = 0o755) -> "Attr":
        """Create a directory.

        Raises:
            DingofsError: on failure.
        """
        parent_path, name = _split(path)
        parent_ino = self._lookup(parent_path)
        uid, gid = _ugid()
        s, attr = self._core.MkDir(parent_ino, name, uid, gid, mode)
        _raise_if_error(s)
        return attr

    def rmdir(self, path: str) -> None:
        """Remove an empty directory.

        Raises:
            DingofsError: on failure.
        """
        parent_path, name = _split(path)
        parent_ino = self._lookup(parent_path)
        s = self._core.RmDir(parent_ino, name)
        _raise_if_error(s)

    def listdir(self, path: str) -> List["DirEntry"]:
        """List directory entries.

        Returns a list of :class:`DirEntry` objects; each has ``name``,
        ``ino``, and ``attr`` fields.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s, fh, _ = self._core.OpenDir(ino)
        _raise_if_error(s)

        entries: list = []

        def _handler(entry, offset: int) -> bool:
            entries.append(entry)
            return True

        s = self._core.ReadDir(ino, fh, 0, True, _handler)
        self._core.ReleaseDir(ino, fh)
        _raise_if_error(s)
        return entries

    def scandir(self, path: str) -> "ScandirIterator":
        """Return a lazy iterator over directory entries in *path*.

        Entries are fetched from the server in batches as the caller
        iterates; no more than ``ScandirIterator._BATCH_SIZE`` entries
        are held in memory at any one time.

        The returned object can also be used as a context manager
        (``with client.scandir(path) as it:``); the directory handle is
        released when the iterator is exhausted or ``close()`` is called.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s, fh, _ = self._core.OpenDir(ino)
        _raise_if_error(s)
        return ScandirIterator(self._core, ino, fh, path)

    def walk(self, top: str, topdown: bool = True):
        """Walk the directory tree rooted at *top*.

        Yields ``(dirpath, dirnames, filenames)`` tuples, mirroring the
        interface of :func:`os.walk`.  Subdirectories that cannot be read
        are silently skipped.

        Args:
            top:     Root path to start the walk.
            topdown: If True (default), yield parent before children;
                     if False, yield children before parent.
        """
        import stat as _stat
        try:
            entries = self.listdir(top)
        except DingofsError:
            return  # unreadable top-level dir: silently skip, like os.walk

        dirnames, filenames = [], []
        for entry in entries:
            if entry.name in (".", ".."):
                continue
            if _stat.S_ISDIR(entry.attr.mode):
                dirnames.append(entry.name)
            else:
                filenames.append(entry.name)

        if topdown:
            yield top, dirnames, filenames
            for name in dirnames:
                yield from self.walk(top.rstrip("/") + "/" + name, topdown=topdown)
        else:
            for name in dirnames:
                yield from self.walk(top.rstrip("/") + "/" + name, topdown=topdown)
            yield top, dirnames, filenames

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    def open(self, path: str, mode: str = "rb") -> DingoFile:
        """Open or create a file and return a :class:`DingoFile` object.

        *mode* is a standard Python mode string (``"r"``, ``"rb"``,
        ``"w"``, ``"wb"``, ``"a"``, ``"r+"``, etc.).

        Raises:
            ValueError:   if *mode* is not a recognised mode string.
            DingofsError: if the file cannot be opened or created.
        """
        if mode not in _MODE_MAP:
            raise ValueError(f"invalid mode {mode!r}")
        flags = _MODE_MAP[mode]
        uid, gid = _ugid()

        if flags & os.O_CREAT:
            parent_path, name = _split(path)
            parent_ino = self._lookup(parent_path)
            s, fh, attr = self._core.Create(
                parent_ino, name, uid, gid, 0o644, flags
            )
            _raise_if_error(s)
            ino = attr.ino
        else:
            ino = self._lookup(path)
            s, fh = self._core.Open(ino, flags)
            _raise_if_error(s)

        return DingoFile(self._core, ino, fh, path, mode)

    def mknod(self, path: str, mode: int, dev: int = 0) -> "Attr":
        """Create a file node.

        Raises:
            DingofsError: on failure.
        """
        parent_path, name = _split(path)
        parent_ino = self._lookup(parent_path)
        uid, gid = _ugid()
        s, attr = self._core.MkNod(parent_ino, name, uid, gid, mode, dev)
        _raise_if_error(s)
        return attr

    def unlink(self, path: str) -> None:
        """Delete a file.

        Raises:
            DingofsError: on failure.
        """
        parent_path, name = _split(path)
        parent_ino = self._lookup(parent_path)
        s = self._core.Unlink(parent_ino, name)
        _raise_if_error(s)

    def rename(self, src: str, dst: str) -> None:
        """Rename or move *src* to *dst*.

        Raises:
            DingofsError: on failure.
        """
        src_parent, src_name = _split(src)
        dst_parent, dst_name = _split(dst)
        src_ino = self._lookup(src_parent)
        dst_ino = self._lookup(dst_parent)
        s = self._core.Rename(src_ino, src_name, dst_ino, dst_name)
        _raise_if_error(s)

    # ------------------------------------------------------------------
    # Hard links and symbolic links
    # ------------------------------------------------------------------

    def link(self, src_path: str, link_path: str) -> "Attr":
        """Create a hard link.

        Raises:
            DingofsError: on failure.
        """
        src_ino = self._lookup(src_path)
        link_parent, link_name = _split(link_path)
        link_parent_ino = self._lookup(link_parent)
        s, attr = self._core.Link(src_ino, link_parent_ino, link_name)
        _raise_if_error(s)
        return attr

    def symlink(self, target: str, link: str) -> "Attr":
        """Create a symbolic link *link* pointing to *target*.

        Raises:
            DingofsError: on failure.
        """
        parent_path, name = _split(link)
        parent_ino = self._lookup(parent_path)
        uid, gid = _ugid()
        s, attr = self._core.Symlink(parent_ino, name, uid, gid, target)
        _raise_if_error(s)
        return attr

    def readlink(self, path: str) -> str:
        """Return the target of a symbolic link.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s, target = self._core.ReadLink(ino)
        _raise_if_error(s)
        return target

    # ------------------------------------------------------------------
    # Extended attributes
    # ------------------------------------------------------------------

    def setxattr(self, path: str, name: str, value: str,
                 flags: int = 0) -> None:
        """Set an extended attribute.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s = self._core.SetXattr(ino, name, value, flags)
        _raise_if_error(s)

    def getxattr(self, path: str, name: str) -> str:
        """Get an extended attribute value.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s, value = self._core.GetXattr(ino, name)
        _raise_if_error(s)
        return value

    def listxattr(self, path: str) -> List[str]:
        """List extended attribute names.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s, names = self._core.ListXattr(ino)
        _raise_if_error(s)
        return names

    def removexattr(self, path: str, name: str) -> None:
        """Remove an extended attribute.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        s = self._core.RemoveXattr(ino, name)
        _raise_if_error(s)

    # ------------------------------------------------------------------
    # Ioctl
    # ------------------------------------------------------------------

    def ioctl(self, path: str, cmd: int, in_buf: bytes = b"",
              out_size: int = 8, flags: int = 0) -> bytes:
        """Perform a filesystem ioctl.

        Raises:
            DingofsError: on failure.
        """
        ino = self._lookup(path)
        uid, _ = _ugid()
        s, out = self._core.Ioctl(ino, uid, cmd, flags, in_buf, out_size)
        _raise_if_error(s)
        return bytes(out)

    # ------------------------------------------------------------------
    # Runtime options (process-wide gflags)
    # ------------------------------------------------------------------

    @staticmethod
    def set_option(key: str, value: str) -> None:
        """Set a tunable SDK option at runtime.

        Raises:
            DingofsError: on failure.
        """
        s = _core_set_option(key, value)
        _raise_if_error(s)

    @staticmethod
    def get_option(key: str) -> str:
        """Get the current value of a tunable SDK option.

        Raises:
            DingofsError: on failure.
        """
        s, value = _core_get_option(key)
        _raise_if_error(s)
        return value

    @staticmethod
    def list_options() -> List["OptionInfo"]:
        """Return all tunable SDK options."""
        return _core_list_options()

    @staticmethod
    def print_options() -> None:
        """Print all tunable SDK options to stdout."""
        _core_print_options()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _lookup(self, path: str) -> int:
        """Resolve *path* to an inode number.

        Walks path components left-to-right using ``BindingClient.Lookup``.

        Raises:
            DingofsError: if any path component cannot be resolved.
        """
        path = os.path.normpath(path)
        if path == "/":
            return _ROOT_INO

        current_ino = _ROOT_INO
        for part in path.lstrip("/").split("/"):
            s, attr = self._core.Lookup(current_ino, part)
            _raise_if_error(s)
            current_ino = attr.ino
        return current_ino

# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _split(path: str):
    """Split ``/a/b/c`` into ``("/a/b", "c")``."""
    path = path.rstrip("/") or "/"
    parent = os.path.dirname(path) or "/"
    name = os.path.basename(path)
    if not name:
        raise ValueError(f"Cannot split root path: {path!r}")
    return parent, name


def _ugid():
    return os.getuid(), os.getgid()
