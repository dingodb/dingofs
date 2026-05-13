# Copyright 2026 DingoDB, Inc.
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

"""Regression tests for backward readdir (seekdir to a previous offset).

Bug:
  DirIterator::GetValue CHECK-failed (crashed) if the readdir offset went
  backward — e.g. seekdir() to a previously-saved position via telldir().

Fix: commit 7a9d0ad9c — Add SeekBackward() with last_name_memo_ that
remembers past fetch boundaries, letting the iterator rewind and re-fetch.

Verification:
  pre fix  (pre 7a9d0ad9c):  test_seekdir_rewind / test_seekdir_after_partial
                             crash or FAIL
  post fix (post 7a9d0ad9c): all tests PASS
  works on: MDS (DirIterator is MDS-specific); Local has a simpler impl
"""

import ctypes
import ctypes.util
import os

import pytest

pytestmark = pytest.mark.standard

# ── libc bindings for opendir/readdir/seekdir/telldir/closedir ────────────

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

class _Dirent(ctypes.Structure):
    _fields_ = [
        ("d_ino", ctypes.c_ulong),
        ("d_off", ctypes.c_long),
        ("d_reclen", ctypes.c_ushort),
        ("d_type", ctypes.c_ubyte),
        ("d_name", ctypes.c_char * 256),
    ]

_libc.opendir.argtypes = [ctypes.c_char_p]
_libc.opendir.restype = ctypes.c_void_p

_libc.readdir.argtypes = [ctypes.c_void_p]
_libc.readdir.restype = ctypes.POINTER(_Dirent)

_libc.telldir.argtypes = [ctypes.c_void_p]
_libc.telldir.restype = ctypes.c_long

_libc.seekdir.argtypes = [ctypes.c_void_p, ctypes.c_long]
_libc.seekdir.restype = None

_libc.closedir.argtypes = [ctypes.c_void_p]
_libc.closedir.restype = ctypes.c_int

def _listdir_raw(path):
    """Read all directory entries via opendir/readdir, return list of names."""
    dirp = _libc.opendir(path.encode())
    if not dirp:
        raise OSError(ctypes.get_errno(), f"opendir failed: {path}")
    names = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            names.append(name)
    _libc.closedir(dirp)
    return sorted(names)

def _readdir_with_seekdir(path, rewind_after=5):
    """Read directory, seekdir back to start after N entries, re-read all.

    Returns (first_pass_names, second_pass_names).
    """
    dirp = _libc.opendir(path.encode())
    if not dirp:
        raise OSError(ctypes.get_errno(), f"opendir failed: {path}")

    # Save position at start
    start_pos = _libc.telldir(dirp)

    # First pass: read some entries
    first_pass = []
    count = 0
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            first_pass.append(name)
            count += 1
            if count >= rewind_after:
                break

    # Seekdir back to start
    _libc.seekdir(dirp, start_pos)

    # Second pass: read all from beginning
    second_pass = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            second_pass.append(name)

    _libc.closedir(dirp)
    return sorted(first_pass), sorted(second_pass)

# ── Tests ─────────────────────────────────────────────────────────────────

def test_listdir_basic(test_dir):
    """Basic: create files, listdir returns all of them."""
    names = {f"file_{i:03d}" for i in range(20)}
    for n in names:
        open(os.path.join(test_dir, n), "w").close()

    listed = set(os.listdir(test_dir))
    assert listed == names

def test_listdir_large(test_dir):
    """Large directory (200 files) — tests multi-batch readdir."""
    names = {f"f_{i:04d}" for i in range(200)}
    for n in names:
        open(os.path.join(test_dir, n), "w").close()

    listed = set(os.listdir(test_dir))
    assert listed == names

def test_seekdir_rewind(test_dir):
    """seekdir(0) after partial read — re-read should return same entries.

    This is the core backward readdir test. Before the fix, this would
    crash the client (CHECK-fail in DirIterator::GetValue).
    """
    names = sorted(f"entry_{i:03d}" for i in range(30))
    for n in names:
        open(os.path.join(test_dir, n), "w").close()

    first_partial, second_full = _readdir_with_seekdir(test_dir, rewind_after=10)

    # First pass should have at least 10 entries
    assert len(first_partial) >= 10

    # Second pass (after seekdir back) should have ALL entries
    assert sorted(second_full) == names

def test_seekdir_after_full_read(test_dir):
    """Read all entries, seekdir(0), read again — both passes identical."""
    names = sorted(f"x_{i:02d}" for i in range(15))
    for n in names:
        open(os.path.join(test_dir, n), "w").close()

    dirp = _libc.opendir(test_dir.encode())
    assert dirp

    start_pos = _libc.telldir(dirp)

    # First full pass
    pass1 = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            pass1.append(name)

    # Rewind
    _libc.seekdir(dirp, start_pos)

    # Second full pass
    pass2 = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            pass2.append(name)

    _libc.closedir(dirp)

    assert sorted(pass1) == names
    assert sorted(pass2) == names

def test_seekdir_multiple_rewinds(test_dir):
    """Multiple seekdir rewinds — should not crash or lose entries."""
    names = sorted(f"m_{i:02d}" for i in range(25))
    for n in names:
        open(os.path.join(test_dir, n), "w").close()

    dirp = _libc.opendir(test_dir.encode())
    assert dirp
    start_pos = _libc.telldir(dirp)

    for _ in range(5):
        # Read a few entries
        count = 0
        while count < 8:
            entry = _libc.readdir(dirp)
            if not entry:
                break
            count += 1

        # Rewind
        _libc.seekdir(dirp, start_pos)

    # Final full read
    final = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            final.append(name)

    _libc.closedir(dirp)

    assert sorted(final) == names

def test_telldir_seekdir_midpoint(test_dir):
    """telldir at midpoint, read rest, seekdir back to midpoint, re-read."""
    names = sorted(f"t_{i:03d}" for i in range(40))
    for n in names:
        open(os.path.join(test_dir, n), "w").close()

    dirp = _libc.opendir(test_dir.encode())
    assert dirp

    # Read 20 entries, save position
    seen_before = []
    for _ in range(22):  # +2 for . and ..
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            seen_before.append(name)

    mid_pos = _libc.telldir(dirp)

    # Read rest
    rest = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            rest.append(name)

    # seekdir back to midpoint
    _libc.seekdir(dirp, mid_pos)

    # Re-read from midpoint
    re_rest = []
    while True:
        entry = _libc.readdir(dirp)
        if not entry:
            break
        name = entry.contents.d_name.decode()
        if name not in (".", ".."):
            re_rest.append(name)

    _libc.closedir(dirp)

    # rest and re_rest should contain the same entries
    assert sorted(rest) == sorted(re_rest)

    # Total should be all names
    assert sorted(seen_before + rest) == names
