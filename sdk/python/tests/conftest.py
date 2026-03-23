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

"""Shared test fixtures and helpers for the DingoFS Python SDK test suite.

All tests use mocks — no real DingoFS cluster is required.
"""

import errno
from unittest.mock import MagicMock

import pytest

from dingofs.file import DingoFile


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def ok_status():
    """Return a mock Status that represents success."""
    s = MagicMock()
    s.ok.return_value = True
    s.ToSysErrNo.return_value = 0
    s.ToString.return_value = "OK"
    return s


def err_status(errno_val=errno.EIO, msg="I/O error"):
    """Return a mock Status that represents failure."""
    s = MagicMock()
    s.ok.return_value = False
    s.ToSysErrNo.return_value = errno_val
    s.ToString.return_value = msg
    return s


# ---------------------------------------------------------------------------
# Attr / DirEntry helpers
# ---------------------------------------------------------------------------

def make_attr(ino=10, length=100, mode=0o644, uid=1000, gid=1000):
    """Return a mock Attr object."""
    attr = MagicMock()
    attr.ino = ino
    attr.length = length
    attr.mode = mode
    attr.uid = uid
    attr.gid = gid
    return attr


def make_dir_entry(name, ino):
    """Return a mock DirEntry."""
    entry = MagicMock()
    entry.name = name
    entry.ino = ino
    entry.attr = make_attr(ino=ino)
    return entry


# ---------------------------------------------------------------------------
# Core mock factory
# ---------------------------------------------------------------------------

def make_core(ino=10, fh=42, file_length=100):
    """Return a mock BindingClient with sensible defaults.

    All methods succeed by default; individual tests override as needed.
    """
    core = MagicMock()
    attr = make_attr(ino=ino, length=file_length)

    core.GetAttr.return_value = (ok_status(), attr)
    core.SetAttr.return_value = (ok_status(), attr)
    core.Flush.return_value = ok_status()
    core.Release.return_value = ok_status()
    core.read_bytes.return_value = (ok_status(), b"x" * file_length)
    core.read_into.return_value = (ok_status(), file_length)
    core.Write.return_value = (ok_status(), 5)

    # Lookup: by default succeed and return a single attr
    core.Lookup.return_value = (ok_status(), attr)

    # Directory operations
    core.StatFs.return_value = (ok_status(), MagicMock())
    core.MkDir.return_value = (ok_status(), attr)
    core.RmDir.return_value = ok_status()
    core.OpenDir.return_value = (ok_status(), fh, attr)
    core.ReadDir.return_value = ok_status()
    core.ReleaseDir.return_value = ok_status()

    # File creation / open
    core.Create.return_value = (ok_status(), fh, attr)
    core.Open.return_value = (ok_status(), fh)

    # File operations
    core.MkNod.return_value = (ok_status(), attr)
    core.Unlink.return_value = ok_status()
    core.Rename.return_value = ok_status()
    core.Fsync.return_value = ok_status()

    # Links
    core.Link.return_value = (ok_status(), attr)
    core.Symlink.return_value = (ok_status(), attr)
    core.ReadLink.return_value = (ok_status(), "/target")

    # Xattrs
    core.SetXattr.return_value = ok_status()
    core.GetXattr.return_value = (ok_status(), "xval")
    core.ListXattr.return_value = (ok_status(), ["user.a", "user.b"])
    core.RemoveXattr.return_value = ok_status()

    # Ioctl
    core.Ioctl.return_value = (ok_status(), bytearray(8))

    return core


# ---------------------------------------------------------------------------
# DingoFile fixture factory
# ---------------------------------------------------------------------------

def make_file(mode="rb", ino=10, fh=42, file_length=100):
    """Return a (DingoFile, core_mock) pair."""
    core = make_core(ino=ino, fh=fh, file_length=file_length)
    f = DingoFile(core, ino=ino, fh=fh, path="/data/file.txt", mode=mode)
    return f, core


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def core():
    return make_core()


@pytest.fixture
def rb_file():
    """Read-only DingoFile."""
    f, core = make_file(mode="rb")
    yield f, core
    if not f.closed:
        f._closed = True  # skip real close in teardown


@pytest.fixture
def wb_file():
    """Write-only DingoFile."""
    f, core = make_file(mode="wb")
    yield f, core
    if not f.closed:
        f._closed = True


@pytest.fixture
def rw_file():
    """Read-write DingoFile."""
    f, core = make_file(mode="r+b")
    yield f, core
    if not f.closed:
        f._closed = True
