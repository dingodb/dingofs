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

"""Tests for the DingoFS Client class.

All tests use a mock BindingClient — no real DingoFS cluster needed.
Strategy:
  - Instantiate Client() directly (bypass build/start).
  - Inject mock core via client._core.
  - Pre-populate client._ino_cache to skip Lookup round-trips where not
    relevant to the test under focus.
"""

import errno
import os
import stat
from unittest.mock import MagicMock, patch, call

import pytest

from dingofs.client import Client, _ROOT_INO, ScandirIterator, _raise_if_error
from dingofs.exceptions import DingofsError
from dingofs.file import DingoFile

from .conftest import (
    ok_status, err_status, make_attr, make_core, make_dir_entry,
)


# ---------------------------------------------------------------------------
# Walk/scandir helpers
# ---------------------------------------------------------------------------

def make_dir_entry_dir(name, ino):
    """DirEntry whose attr.mode marks it as a directory."""
    e = make_dir_entry(name, ino)
    e.attr.mode = stat.S_IFDIR | 0o755
    return e


def make_dir_entry_file(name, ino):
    """DirEntry whose attr.mode marks it as a regular file."""
    e = make_dir_entry(name, ino)
    e.attr.mode = stat.S_IFREG | 0o644
    return e


# ---------------------------------------------------------------------------
# Helper: build a pre-wired client without touching real C++ code
# ---------------------------------------------------------------------------

def make_client(paths=None, ino=10, fh=42, file_length=100):
    """Return (client, core_mock).

    paths is accepted but ignored (no inode cache in Python layer).
    """
    c = Client()
    c._core = make_core(ino=ino, fh=fh, file_length=file_length)
    return c, c._core


# ===========================================================================
# build() / start() / stop()
# ===========================================================================

class TestLifecycle:
    def test_build_returns_client(self):
        with patch("dingofs.client.BindingClient") as MockBC, \
             patch("dingofs.client.BindingConfig") as MockBCfg:
            mock_core = make_core()
            MockBC.return_value = mock_core
            mock_core.Start.return_value = ok_status()

            c = Client.build(
                mds_addrs="127.0.0.1:8801",
                fs_name="test",
                mount_point="/mnt/test",
            )
            assert isinstance(c, Client)

    def test_build_raises_on_start_failure(self):
        with patch("dingofs.client.BindingClient") as MockBC, \
             patch("dingofs.client.BindingConfig"):
            mock_core = make_core()
            MockBC.return_value = mock_core
            mock_core.Start.return_value = err_status(errno.ECONNREFUSED, "conn refused")

            with pytest.raises(DingofsError) as exc_info:
                Client.build(
                    mds_addrs="127.0.0.1:8801",
                    fs_name="test",
                    mount_point="/mnt/test",
                )
            assert exc_info.value.dingofs_errno == errno.ECONNREFUSED

    def test_stop_clears_core(self):
        c, core = make_client()
        core.Stop.return_value = ok_status()
        c.stop()
        assert c._core is None

    def test_stop_is_noop_when_not_started(self):
        c = Client()
        c._core = None
        c.stop()  # must not raise

    def test_stop_raises_on_failure(self):
        c, core = make_client()
        core.Stop.return_value = err_status(errno.EIO, "stop failed")
        with pytest.raises(DingofsError):
            c.stop()


# ===========================================================================
# statfs()
# ===========================================================================

class TestStatfs:
    def test_statfs_returns_fsstat(self):
        c, core = make_client()
        fsstat = MagicMock()
        core.StatFs.return_value = (ok_status(), fsstat)
        result = c.statfs()
        assert result is fsstat
        core.StatFs.assert_called_once_with(_ROOT_INO)

    def test_statfs_raises_on_error(self):
        c, core = make_client()
        core.StatFs.return_value = (err_status(errno.EIO), MagicMock())
        with pytest.raises(DingofsError):
            c.statfs()


# ===========================================================================
# stat() / set_attr() / chmod() / chown()
# ===========================================================================

class TestAttr:
    def test_stat_returns_attr(self):
        c, core = make_client(paths={"/file.txt": 10})
        attr = make_attr(ino=10)
        core.GetAttr.return_value = (ok_status(), attr)
        result = c.stat("/file.txt")
        assert result is attr

    def test_stat_missing_path_raises(self):
        c, core = make_client()
        core.Lookup.return_value = (err_status(errno.ENOENT, "not found"), MagicMock())
        with pytest.raises(DingofsError) as exc_info:
            c.stat("/missing")
        assert exc_info.value.dingofs_errno == errno.ENOENT

    def test_set_attr_returns_updated_attr(self):
        c, core = make_client(paths={"/file.txt": 10})
        updated = make_attr(mode=0o755)
        core.SetAttr.return_value = (ok_status(), updated)
        in_attr = MagicMock()
        result = c.set_attr("/file.txt", 0x01, in_attr)
        assert result is updated

    def test_chmod_sets_mode_mask(self):
        c, core = make_client(paths={"/file.txt": 10})
        from dingofs.client import SET_ATTR_MODE
        updated = make_attr(mode=0o755)
        core.SetAttr.return_value = (ok_status(), updated)
        result = c.chmod("/file.txt", 0o755)
        assert result is updated
        args = core.SetAttr.call_args
        assert args[0][1] == SET_ATTR_MODE

    def test_chown_uid_and_gid(self):
        c, core = make_client(paths={"/file.txt": 10})
        from dingofs.client import SET_ATTR_UID, SET_ATTR_GID
        core.SetAttr.return_value = (ok_status(), make_attr())
        c.chown("/file.txt", 500, 600)
        args = core.SetAttr.call_args[0]
        assert args[1] == SET_ATTR_UID | SET_ATTR_GID

    def test_chown_uid_only(self):
        c, core = make_client(paths={"/file.txt": 10})
        from dingofs.client import SET_ATTR_UID, SET_ATTR_GID
        core.SetAttr.return_value = (ok_status(), make_attr())
        c.chown("/file.txt", 500, -1)
        args = core.SetAttr.call_args[0]
        assert args[1] == SET_ATTR_UID
        assert not (args[1] & SET_ATTR_GID)

    def test_chown_neither_calls_getattr(self):
        c, core = make_client(paths={"/file.txt": 10})
        attr = make_attr()
        core.GetAttr.return_value = (ok_status(), attr)
        result = c.chown("/file.txt", -1, -1)
        core.SetAttr.assert_not_called()
        assert result is attr


# ===========================================================================
# mkdir() / rmdir() / listdir()
# ===========================================================================

class TestDirectory:
    def test_mkdir_returns_attr(self):
        c, core = make_client(paths={"/data": 5})
        attr = make_attr(ino=20)
        core.MkDir.return_value = (ok_status(), attr)
        result = c.mkdir("/data/sub", 0o755)
        assert result is attr

    def test_mkdir_parent_not_found_raises(self):
        c, core = make_client()
        core.Lookup.return_value = (err_status(errno.ENOENT), MagicMock())
        with pytest.raises(DingofsError):
            c.mkdir("/missing/sub")

    def test_rmdir_raises_on_error(self):
        c, core = make_client(paths={"/data": 5, "/data/sub": 20})
        core.RmDir.return_value = err_status(errno.ENOTEMPTY, "not empty")
        with pytest.raises(DingofsError) as exc_info:
            c.rmdir("/data/sub")
        assert exc_info.value.dingofs_errno == errno.ENOTEMPTY

    def test_listdir_returns_entries(self):
        c, core = make_client(paths={"/data": 5})
        entries = [make_dir_entry("foo", 11), make_dir_entry("bar", 12)]

        def fake_readdir(ino, fh, offset, fill_stat, handler):
            for e in entries:
                handler(e, 0)
            return ok_status()

        core.ReadDir.side_effect = fake_readdir
        result = c.listdir("/data")
        assert len(result) == 2
        assert result[0].name == "foo"
        assert result[1].name == "bar"

    def test_listdir_opendir_error_raises(self):
        c, core = make_client(paths={"/data": 5})
        core.OpenDir.return_value = (err_status(errno.EACCES), 0, MagicMock())
        with pytest.raises(DingofsError):
            c.listdir("/data")


# ===========================================================================
# open()
# ===========================================================================

class TestOpen:
    def test_open_rb_returns_dingofile(self):
        c, core = make_client(paths={"/file.txt": 10})
        f = c.open("/file.txt", "rb")
        assert isinstance(f, DingoFile)
        assert f.mode == "rb"
        assert not f.closed

    def test_open_wb_creates_file(self):
        c, core = make_client(paths={"/data": 5})
        attr = make_attr(ino=20)
        core.Create.return_value = (ok_status(), 42, attr)
        f = c.open("/data/new.txt", "wb")
        assert isinstance(f, DingoFile)
        core.Create.assert_called_once()

    def test_open_invalid_mode_raises_valueerror(self):
        c, _ = make_client()
        with pytest.raises(ValueError, match="invalid mode"):
            c.open("/file.txt", "x")

    def test_open_missing_file_raises_dingofserror(self):
        c, core = make_client()
        core.Lookup.return_value = (err_status(errno.ENOENT, "not found"), MagicMock())
        with pytest.raises(DingofsError) as exc_info:
            c.open("/missing.txt", "rb")
        assert exc_info.value.dingofs_errno == errno.ENOENT

    def test_open_core_error_raises_dingofserror(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.Open.return_value = (err_status(errno.EACCES, "permission denied"), 0)
        with pytest.raises(DingofsError) as exc_info:
            c.open("/file.txt", "rb")
        assert exc_info.value.dingofs_errno == errno.EACCES

    @pytest.mark.parametrize("mode", [
        "r", "rb", "w", "wb", "a", "ab",
        "r+", "r+b", "rb+", "w+", "w+b", "wb+",
        "a+", "a+b", "ab+",
    ])
    def test_open_all_valid_modes(self, mode):
        """Every mode in _MODE_MAP should return a DingoFile without error."""
        c, core = make_client(paths={"/data": 5, "/data/f.txt": 10})
        attr = make_attr(ino=10)
        core.Create.return_value = (ok_status(), 42, attr)
        core.Open.return_value = (ok_status(), 42)
        f = c.open("/data/f.txt", mode)
        assert isinstance(f, DingoFile)


# ===========================================================================
# mknod() / unlink() / rename()
# ===========================================================================

class TestFileOps:
    def test_mknod_returns_attr(self):
        c, core = make_client(paths={"/data": 5})
        attr = make_attr(ino=20)
        core.MkNod.return_value = (ok_status(), attr)
        result = c.mknod("/data/node", 0o644)
        assert result is attr

    def test_unlink_raises_on_error(self):
        c, core = make_client(paths={"/data": 5, "/data/file.txt": 10})
        core.Unlink.return_value = err_status(errno.ENOENT, "not found")
        with pytest.raises(DingofsError):
            c.unlink("/data/file.txt")

    def test_rename_raises_on_error(self):
        c, core = make_client(paths={"/data": 5, "/backup": 6})
        core.Rename.return_value = err_status(errno.EXDEV, "cross-device")
        with pytest.raises(DingofsError):
            c.rename("/data/a.txt", "/backup/a.txt")


# ===========================================================================
# link() / symlink() / readlink()
# ===========================================================================

class TestLinks:
    def test_link_returns_attr(self):
        c, core = make_client()
        attr = make_attr(ino=10)
        core.Link.return_value = (ok_status(), attr)
        result = c.link("/src.txt", "/data/hard.txt")
        assert result is attr

    def test_symlink_returns_attr(self):
        c, core = make_client()
        attr = make_attr(ino=30)
        core.Symlink.return_value = (ok_status(), attr)
        result = c.symlink("/target.txt", "/data/link.txt")
        assert result is attr

    def test_readlink_returns_target(self):
        c, core = make_client(paths={"/data/link.txt": 30})
        core.ReadLink.return_value = (ok_status(), "/target.txt")
        result = c.readlink("/data/link.txt")
        assert result == "/target.txt"

    def test_readlink_raises_on_error(self):
        c, core = make_client(paths={"/data/link.txt": 30})
        core.ReadLink.return_value = (err_status(errno.EINVAL, "not a symlink"), "")
        with pytest.raises(DingofsError):
            c.readlink("/data/link.txt")


# ===========================================================================
# setxattr / getxattr / listxattr / removexattr
# ===========================================================================

class TestXattr:
    def test_setxattr(self):
        c, core = make_client(paths={"/file.txt": 10})
        c.setxattr("/file.txt", "user.tag", "value")
        core.SetXattr.assert_called_once_with(10, "user.tag", "value", 0)

    def test_setxattr_raises_on_error(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.SetXattr.return_value = err_status(errno.ENOSPC)
        with pytest.raises(DingofsError):
            c.setxattr("/file.txt", "user.tag", "v")

    def test_getxattr_returns_value(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.GetXattr.return_value = (ok_status(), "myvalue")
        result = c.getxattr("/file.txt", "user.tag")
        assert result == "myvalue"

    def test_getxattr_raises_on_error(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.GetXattr.return_value = (err_status(errno.ENODATA, "no attr"), "")
        with pytest.raises(DingofsError):
            c.getxattr("/file.txt", "user.tag")

    def test_listxattr_returns_list(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.ListXattr.return_value = (ok_status(), ["user.a", "user.b"])
        result = c.listxattr("/file.txt")
        assert result == ["user.a", "user.b"]

    def test_removexattr(self):
        c, core = make_client(paths={"/file.txt": 10})
        c.removexattr("/file.txt", "user.tag")
        core.RemoveXattr.assert_called_once_with(10, "user.tag")

    def test_removexattr_raises_on_error(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.RemoveXattr.return_value = err_status(errno.ENODATA)
        with pytest.raises(DingofsError):
            c.removexattr("/file.txt", "user.tag")


# ===========================================================================
# ioctl()
# ===========================================================================

class TestIoctl:
    def test_ioctl_returns_bytes(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.Ioctl.return_value = (ok_status(), bytearray(b"\x01\x02"))
        result = c.ioctl("/file.txt", cmd=1)
        assert isinstance(result, bytes)
        assert result == b"\x01\x02"

    def test_ioctl_raises_on_error(self):
        c, core = make_client(paths={"/file.txt": 10})
        core.Ioctl.return_value = (err_status(errno.EINVAL), bytearray())
        with pytest.raises(DingofsError):
            c.ioctl("/file.txt", cmd=999)


# ===========================================================================
# set_option() / get_option()
# ===========================================================================

class TestOptions:
    def test_set_option(self):
        with patch("dingofs.client._core_set_option") as mock_set:
            mock_set.return_value = ok_status()
            Client.set_option("key", "value")
            mock_set.assert_called_once_with("key", "value")

    def test_set_option_raises_on_error(self):
        with patch("dingofs.client._core_set_option") as mock_set:
            mock_set.return_value = err_status(errno.EINVAL, "unknown key")
            with pytest.raises(DingofsError):
                Client.set_option("bad_key", "val")

    def test_get_option(self):
        with patch("dingofs.client._core_get_option") as mock_get:
            mock_get.return_value = (ok_status(), "64")
            result = Client.get_option("block_size")
            assert result == "64"

    def test_get_option_raises_on_error(self):
        with patch("dingofs.client._core_get_option") as mock_get:
            mock_get.return_value = (err_status(errno.EINVAL), "")
            with pytest.raises(DingofsError):
                Client.get_option("bad_key")


# ===========================================================================
# _lookup() internals
# ===========================================================================

class TestLookup:
    def test_lookup_root(self):
        c, _ = make_client()
        ino = c._lookup("/")
        assert ino == _ROOT_INO

    def test_lookup_walks_components(self):
        c, core = make_client()
        attrs = [make_attr(ino=5), make_attr(ino=10)]
        core.Lookup.side_effect = [(ok_status(), a) for a in attrs]
        ino = c._lookup("/data/file.txt")
        assert ino == 10
        assert core.Lookup.call_count == 2

    def test_lookup_always_calls_lookup(self):
        c, core = make_client()
        attrs = [make_attr(ino=5), make_attr(ino=10)]
        core.Lookup.side_effect = [(ok_status(), a) for a in attrs]
        c._lookup("/data/file.txt")
        # No cache: Lookup is called for every component on every call
        assert core.Lookup.call_count == 2

    def test_lookup_raises_on_missing_component(self):
        c, core = make_client()
        core.Lookup.return_value = (err_status(errno.ENOENT, "not found"), MagicMock())
        with pytest.raises(DingofsError) as exc_info:
            c._lookup("/missing/path")
        assert exc_info.value.dingofs_errno == errno.ENOENT


# ===========================================================================
# scandir()
# ===========================================================================

class TestScandir:
    """ScandirIterator fetches via ReadDir in batches; tests mock the core."""

    @staticmethod
    def _setup_readdir(core, entries):
        """Wire core.ReadDir to serve *entries* with index-based offsets.

        Offset semantics: handler is called with offset = i+1, meaning
        "pass i+1 to the next ReadDir call to continue after entry i".
        ReadDir(offset=k) starts from entries[k].
        """
        def fake_readdir(ino, fh, offset, with_attr, handler):
            for i in range(int(offset), len(entries)):
                if not handler(entries[i], i + 1):
                    break
            return ok_status()

        core.ReadDir.side_effect = fake_readdir

    def test_scandir_yields_entries(self):
        c, core = make_client(paths={"/data": 5})
        entries = [make_dir_entry("foo", 11), make_dir_entry("bar", 12)]
        self._setup_readdir(core, entries)
        result = list(c.scandir("/data"))
        assert len(result) == 2
        assert result[0].name == "foo"
        assert result[1].name == "bar"

    def test_scandir_as_context_manager(self):
        c, core = make_client(paths={"/data": 5})
        entries = [make_dir_entry("foo", 11)]
        self._setup_readdir(core, entries)
        collected = []
        with c.scandir("/data") as it:
            for entry in it:
                collected.append(entry.name)
        assert collected == ["foo"]
        core.ReleaseDir.assert_called_once()

    def test_scandir_close_releases_dir(self):
        c, core = make_client(paths={"/data": 5})
        entries = [make_dir_entry("a", 11), make_dir_entry("b", 12)]
        self._setup_readdir(core, entries)
        it = c.scandir("/data")
        next(it)        # consume one entry
        it.close()      # release handle before iteration finishes
        core.ReleaseDir.assert_called_once()
        assert list(it) == []   # exhausted after close

    def test_scandir_opendir_error_propagates(self):
        c, core = make_client(paths={"/data": 5})
        core.OpenDir.return_value = (err_status(errno.EACCES), 0, MagicMock())
        with pytest.raises(DingofsError) as exc_info:
            c.scandir("/data")
        assert exc_info.value.dingofs_errno == errno.EACCES

    def test_scandir_batched_fetch(self):
        """Entries exceeding _BATCH_SIZE trigger multiple ReadDir calls."""
        c, core = make_client(paths={"/data": 5})
        n = ScandirIterator._BATCH_SIZE + 10
        entries = [make_dir_entry(f"f{i}", 100 + i) for i in range(n)]
        self._setup_readdir(core, entries)
        result = list(c.scandir("/data"))
        assert len(result) == n
        assert core.ReadDir.call_count >= 2


# ===========================================================================
# walk()
# ===========================================================================

class TestWalk:
    def _make_listdir(self, mapping):
        """Return a listdir replacement driven by a path→entries dict."""
        def listdir(path):
            if path not in mapping:
                raise DingofsError(errno.ENOENT, "not found")
            return mapping[path]
        return listdir

    def test_walk_topdown(self, monkeypatch):
        c, _ = make_client()
        mapping = {
            "/root": [
                make_dir_entry_dir("sub", 20),
                make_dir_entry_file("a.txt", 11),
            ],
            "/root/sub": [
                make_dir_entry_file("b.txt", 30),
            ],
        }
        monkeypatch.setattr(c, "listdir", self._make_listdir(mapping))
        results = list(c.walk("/root"))
        # topdown=True: root appears before sub
        assert results[0][0] == "/root"
        assert results[1][0] == "/root/sub"
        assert "sub" in results[0][1]
        assert "a.txt" in results[0][2]
        assert "b.txt" in results[1][2]

    def test_walk_bottomup(self, monkeypatch):
        c, _ = make_client()
        mapping = {
            "/root": [
                make_dir_entry_dir("sub", 20),
                make_dir_entry_file("a.txt", 11),
            ],
            "/root/sub": [
                make_dir_entry_file("b.txt", 30),
            ],
        }
        monkeypatch.setattr(c, "listdir", self._make_listdir(mapping))
        results = list(c.walk("/root", topdown=False))
        # bottomup: sub appears before root
        assert results[0][0] == "/root/sub"
        assert results[1][0] == "/root"

    def test_walk_filters_dot_dotdot(self, monkeypatch):
        c, _ = make_client()
        mapping = {
            "/root": [
                make_dir_entry_dir(".", 1),
                make_dir_entry_dir("..", 1),
                make_dir_entry_file("real.txt", 11),
            ],
        }
        monkeypatch.setattr(c, "listdir", self._make_listdir(mapping))
        results = list(c.walk("/root"))
        assert len(results) == 1
        dirpath, dirnames, filenames = results[0]
        assert "." not in dirnames and ".." not in dirnames
        assert "." not in filenames and ".." not in filenames
        assert "real.txt" in filenames

    def test_walk_skips_unreadable_subdir(self, monkeypatch):
        c, _ = make_client()
        # /root has a sub-directory; that sub-directory raises on listdir
        root_entries = [
            make_dir_entry_dir("sub", 20),
            make_dir_entry_file("a.txt", 11),
        ]
        def listdir(path):
            if path == "/root":
                return root_entries
            raise DingofsError(errno.EACCES, "permission denied")
        monkeypatch.setattr(c, "listdir", listdir)
        results = list(c.walk("/root"))
        # sub is silently skipped; only root is yielded
        paths = [r[0] for r in results]
        assert "/root" in paths
        assert "/root/sub" not in paths

    def test_walk_empty_dir(self, monkeypatch):
        c, _ = make_client()
        monkeypatch.setattr(c, "listdir", lambda path: [])
        results = list(c.walk("/empty"))
        assert results == [("/empty", [], [])]
