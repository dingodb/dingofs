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

"""Tests for DingoFile — the file-like object returned by Client.open().

All tests use a mock core (BindingClient) — no real DingoFS cluster needed.
"""

import errno
from unittest.mock import call

import pytest

from dingofs.exceptions import DingofsError
from dingofs.file import DingoFile

from .conftest import (
    err_status, ok_status, make_attr, make_file, make_core,
)


# ===========================================================================
# Properties
# ===========================================================================

class TestProperties:
    def test_name(self):
        f, _ = make_file()
        assert f.name == "/data/file.txt"

    def test_mode(self):
        f, _ = make_file(mode="wb")
        assert f.mode == "wb"

    def test_closed_initially_false(self):
        f, _ = make_file()
        assert f.closed is False

    def test_closed_true_after_close(self):
        f, _ = make_file()
        f.close()
        assert f.closed is True

    def test_repr_open(self):
        f, _ = make_file()
        r = repr(f)
        assert "open" in r
        assert "/data/file.txt" in r

    def test_repr_closed(self):
        f, _ = make_file()
        f.close()
        assert "closed" in repr(f)


# ===========================================================================
# IO capability flags
# ===========================================================================

class TestCapabilities:
    @pytest.mark.parametrize("mode,readable,writable", [
        ("rb",  True,  False),
        ("wb",  False, True),
        ("ab",  False, True),
        ("r+b", True,  True),
        ("w+b", True,  True),
        ("a+b", True,  True),
    ])
    def test_readable_writable(self, mode, readable, writable):
        f, _ = make_file(mode=mode)
        assert f.readable() == readable
        assert f.writable() == writable

    def test_seekable_always_true(self):
        for mode in ("rb", "wb", "ab", "r+b"):
            f, _ = make_file(mode=mode)
            assert f.seekable() is True


# ===========================================================================
# read()
# ===========================================================================

class TestRead:
    def test_read_n_bytes(self, rb_file):
        f, core = rb_file
        core.read_bytes.return_value = (ok_status(), b"hello")
        data = f.read(5)
        assert data == b"hello"

    def test_read_advances_offset(self, rb_file):
        f, core = rb_file
        core.read_bytes.return_value = (ok_status(), b"abcde")
        f.read(5)
        assert f.tell() == 5

    def test_read_minus_one_reads_to_eof(self, rb_file):
        f, core = rb_file
        # file_length=100, offset starts at 0 → should request 100 bytes
        core.read_bytes.return_value = (ok_status(), b"x" * 100)
        data = f.read(-1)
        assert len(data) == 100
        core.read_bytes.assert_called_once_with(10, 100, 0, 42)

    def test_read_minus_one_at_eof_returns_empty(self, rb_file):
        f, core = rb_file
        # position at end of file
        attr = make_attr(length=50)
        core.GetAttr.return_value = (ok_status(), attr)
        f._offset = 50
        data = f.read(-1)
        assert data == b""
        core.read_bytes.assert_not_called()

    def test_read_zero_bytes(self, rb_file):
        f, core = rb_file
        data = f.read(0)
        assert data == b""
        core.read_bytes.assert_not_called()

    def test_read_chained_calls_chain_offset(self, rb_file):
        f, core = rb_file
        core.read_bytes.side_effect = [
            (ok_status(), b"hello"),
            (ok_status(), b" world"),
        ]
        assert f.read(5) == b"hello"
        assert f.read(6) == b" world"
        assert f.tell() == 11

    def test_read_with_null_bytes(self, rb_file):
        f, core = rb_file
        core.read_bytes.return_value = (ok_status(), b"\x00abc\x00")
        data = f.read(5)
        assert data == b"\x00abc\x00"

    def test_read_uses_correct_arguments(self, rb_file):
        f, core = rb_file
        f._offset = 20
        core.read_bytes.return_value = (ok_status(), b"xyz")
        f.read(3)
        core.read_bytes.assert_called_once_with(10, 3, 20, 42)

    def test_read_error_raises_dingofserror(self, rb_file):
        f, core = rb_file
        core.read_bytes.return_value = (err_status(errno.EIO, "IO error"), b"")
        with pytest.raises(DingofsError) as exc_info:
            f.read(10)
        assert exc_info.value.dingofs_errno == errno.EIO

    def test_read_on_write_only_raises_ebadf(self, wb_file):
        f, _ = wb_file
        with pytest.raises(OSError) as exc_info:
            f.read(10)
        assert exc_info.value.errno == errno.EBADF

    def test_read_on_closed_raises_valueerror(self, rb_file):
        f, _ = rb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.read(10)


# ===========================================================================
# write()
# ===========================================================================

class TestWrite:
    def test_write_returns_byte_count(self, wb_file):
        f, core = wb_file
        core.Write.return_value = (ok_status(), 5)
        n = f.write(b"hello")
        assert n == 5

    def test_write_advances_offset(self, wb_file):
        f, core = wb_file
        core.Write.return_value = (ok_status(), 5)
        f.write(b"hello")
        assert f.tell() == 5

    def test_write_uses_correct_arguments(self, wb_file):
        f, core = wb_file
        f._offset = 10
        core.Write.return_value = (ok_status(), 3)
        f.write(b"abc")
        core.Write.assert_called_once_with(10, b"abc", 3, 10, 42)

    def test_write_null_bytes(self, wb_file):
        f, core = wb_file
        core.Write.return_value = (ok_status(), 5)
        n = f.write(b"\x00abc\x00")
        assert n == 5

    def test_write_append_mode_seeks_end_first(self):
        """Append mode: each write queries GetAttr to find EOF."""
        f, core = make_file(mode="ab", file_length=50)
        core.Write.return_value = (ok_status(), 5)
        f.write(b"hello")
        # GetAttr must be called to find current EOF
        core.GetAttr.assert_called_once()
        # Write must start at EOF offset (50)
        core.Write.assert_called_once_with(10, b"hello", 5, 50, 42)

    def test_write_error_raises_dingofserror(self, wb_file):
        f, core = wb_file
        core.Write.return_value = (err_status(errno.ENOSPC, "no space"), 0)
        with pytest.raises(DingofsError) as exc_info:
            f.write(b"data")
        assert exc_info.value.dingofs_errno == errno.ENOSPC

    def test_write_on_read_only_raises_ebadf(self, rb_file):
        f, _ = rb_file
        with pytest.raises(OSError) as exc_info:
            f.write(b"data")
        assert exc_info.value.errno == errno.EBADF

    def test_write_on_closed_raises_valueerror(self, wb_file):
        f, _ = wb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.write(b"data")


# ===========================================================================
# seek() / tell()
# ===========================================================================

class TestSeek:
    def test_seek_set(self, rb_file):
        f, _ = rb_file
        pos = f.seek(42, 0)
        assert pos == 42
        assert f.tell() == 42

    def test_seek_cur_positive(self, rb_file):
        f, _ = rb_file
        f._offset = 10
        pos = f.seek(5, 1)
        assert pos == 15

    def test_seek_cur_negative(self, rb_file):
        f, _ = rb_file
        f._offset = 10
        pos = f.seek(-3, 1)
        assert pos == 7

    def test_seek_end(self, rb_file):
        f, core = rb_file
        attr = make_attr(length=200)
        core.GetAttr.return_value = (ok_status(), attr)
        pos = f.seek(0, 2)
        assert pos == 200

    def test_seek_end_with_offset(self, rb_file):
        f, core = rb_file
        attr = make_attr(length=200)
        core.GetAttr.return_value = (ok_status(), attr)
        pos = f.seek(-10, 2)
        assert pos == 190

    def test_seek_negative_absolute_raises_einval(self, rb_file):
        f, _ = rb_file
        with pytest.raises(OSError) as exc_info:
            f.seek(-1, 0)
        assert exc_info.value.errno == errno.EINVAL

    def test_seek_cur_past_negative_raises_einval(self, rb_file):
        f, _ = rb_file
        f._offset = 5
        with pytest.raises(OSError) as exc_info:
            f.seek(-10, 1)
        assert exc_info.value.errno == errno.EINVAL

    def test_seek_invalid_whence_raises_einval(self, rb_file):
        f, _ = rb_file
        with pytest.raises(OSError) as exc_info:
            f.seek(0, 99)
        assert exc_info.value.errno == errno.EINVAL

    def test_tell_returns_current_offset(self, rb_file):
        f, _ = rb_file
        f._offset = 77
        assert f.tell() == 77

    def test_seek_on_closed_raises_valueerror(self, rb_file):
        f, _ = rb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.seek(0)

    def test_tell_on_closed_raises_valueerror(self, rb_file):
        f, _ = rb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.tell()


# ===========================================================================
# flush()
# ===========================================================================

class TestFlush:
    def test_flush_calls_core(self, wb_file):
        f, core = wb_file
        f.flush()
        core.Flush.assert_called_once_with(10, 42)

    def test_flush_error_raises_dingofserror(self, wb_file):
        f, core = wb_file
        core.Flush.return_value = err_status(errno.EIO, "flush failed")
        with pytest.raises(DingofsError):
            f.flush()

    def test_flush_on_closed_raises_valueerror(self, wb_file):
        f, _ = wb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.flush()


# ===========================================================================
# fsync()
# ===========================================================================

class TestFsync:
    def test_fsync_calls_core(self, wb_file):
        f, core = wb_file
        f.fsync()
        core.Fsync.assert_called_once_with(10, 0, 42)

    def test_fsync_datasync_true(self, wb_file):
        f, core = wb_file
        f.fsync(datasync=True)
        core.Fsync.assert_called_once_with(10, 1, 42)

    def test_fsync_error_raises(self, wb_file):
        f, core = wb_file
        core.Fsync.return_value = err_status(errno.EIO, "sync failed")
        with pytest.raises(DingofsError):
            f.fsync()

    def test_fsync_on_closed_raises_valueerror(self, wb_file):
        f, _ = wb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.fsync()


# ===========================================================================
# close()
# ===========================================================================

class TestClose:
    def test_close_calls_flush_then_release(self, wb_file):
        f, core = wb_file
        f.close()
        core.Flush.assert_called_once_with(10, 42)
        core.Release.assert_called_once_with(10, 42)

    def test_close_idempotent(self, wb_file):
        f, core = wb_file
        f.close()
        f.close()
        f.close()
        # Flush and Release each called only once
        assert core.Flush.call_count == 1
        assert core.Release.call_count == 1

    def test_close_release_called_even_if_flush_fails(self, wb_file):
        """Release must execute even when Flush raises (finally block)."""
        f, core = wb_file
        core.Flush.return_value = err_status(errno.EIO, "flush error")
        with pytest.raises(DingofsError):
            f.close()
        # Release still called despite Flush error
        core.Release.assert_called_once_with(10, 42)

    def test_close_marks_closed_before_c_calls(self, wb_file):
        """_closed is set to True before Flush/Release to prevent re-entry."""
        f, core = wb_file

        def side_effect(*_):
            # At the moment Flush is called, f must already be marked closed
            assert f.closed is True
            return ok_status()

        core.Flush.side_effect = side_effect
        f.close()


# ===========================================================================
# Context manager
# ===========================================================================

class TestContextManager:
    def test_enter_returns_self(self, rb_file):
        f, _ = rb_file
        with f as g:
            assert g is f

    def test_exit_closes_file(self):
        f, core = make_file()
        with f:
            pass
        assert f.closed is True
        core.Release.assert_called_once()

    def test_exit_closes_file_on_exception(self):
        f, core = make_file()
        with pytest.raises(RuntimeError):
            with f:
                raise RuntimeError("boom")
        assert f.closed is True
        core.Release.assert_called_once()


# ===========================================================================
# Integration: read → seek → read
# ===========================================================================

class TestIntegration:
    def test_read_seek_read(self, rb_file):
        f, core = rb_file
        core.read_bytes.side_effect = [
            (ok_status(), b"hello"),
            (ok_status(), b"world"),
        ]
        assert f.read(5) == b"hello"
        f.seek(0, 0)
        assert f.tell() == 0
        assert f.read(5) == b"world"

    def test_write_then_seek_to_start(self, rw_file):
        f, core = rw_file
        core.Write.return_value = (ok_status(), 5)
        core.read_bytes.return_value = (ok_status(), b"hello")
        f.write(b"hello")
        assert f.tell() == 5
        f.seek(0)
        assert f.tell() == 0
        f.read(5)
        core.read_bytes.assert_called_once_with(10, 5, 0, 42)


# ===========================================================================
# read_into()
# ===========================================================================

class TestReadInto:
    def test_read_into_returns_byte_count(self, rb_file):
        f, core = rb_file
        core.read_into.return_value = (ok_status(), 5)
        buf = bytearray(5)
        n = f.read_into(buf)
        assert n == 5

    def test_read_into_advances_offset(self, rb_file):
        f, core = rb_file
        core.read_into.return_value = (ok_status(), 8)
        buf = bytearray(8)
        f.read_into(buf)
        assert f.tell() == 8

    def test_read_into_uses_buf_len_as_default_size(self, rb_file):
        f, core = rb_file
        f._offset = 10
        core.read_into.return_value = (ok_status(), 16)
        buf = bytearray(16)
        f.read_into(buf)
        core.read_into.assert_called_once_with(10, buf, 16, 10, 42)

    def test_read_into_respects_size_argument(self, rb_file):
        f, core = rb_file
        f._offset = 0
        core.read_into.return_value = (ok_status(), 4)
        buf = bytearray(16)
        n = f.read_into(buf, size=4)
        assert n == 4
        core.read_into.assert_called_once_with(10, buf, 4, 0, 42)

    def test_read_into_zero_size_skips_core(self, rb_file):
        f, core = rb_file
        buf = bytearray(16)
        n = f.read_into(buf, size=0)
        assert n == 0
        core.read_into.assert_not_called()

    def test_read_into_error_raises_dingofserror(self, rb_file):
        f, core = rb_file
        core.read_into.return_value = (err_status(errno.EIO, "IO error"), 0)
        buf = bytearray(8)
        with pytest.raises(DingofsError) as exc_info:
            f.read_into(buf)
        assert exc_info.value.dingofs_errno == errno.EIO

    def test_read_into_on_write_only_raises_ebadf(self, wb_file):
        f, _ = wb_file
        with pytest.raises(OSError) as exc_info:
            f.read_into(bytearray(8))
        assert exc_info.value.errno == errno.EBADF

    def test_read_into_on_closed_raises_valueerror(self, rb_file):
        f, _ = rb_file
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.read_into(bytearray(8))

    def test_read_into_memoryview_accepted(self, rb_file):
        f, core = rb_file
        core.read_into.return_value = (ok_status(), 8)
        buf = bytearray(8)
        n = f.read_into(memoryview(buf))
        assert n == 8
