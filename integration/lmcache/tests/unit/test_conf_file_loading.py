# SPDX-License-Identifier: Apache-2.0
"""End-to-end tests for the dingofs conf-file loading path.

These exercise the real native module (``_dingofs_native``) — they verify
that the conf-file path passed through the Python layers actually reaches
``gflags::ReadFromFlagsFile`` and mutates process-global gflag state.

Skipped automatically when the native ``.so`` isn't installed (the rest of
the unit test suite has no native dependency).

We never call ``RemoteCache`` against a real cluster: we point ``mds_addrs``
at an unrouteable endpoint so ``StartCache`` always fails. ``InstallFlags``
(where the conf file is consumed) runs *before* ``StartCache``, so the
flags-loading observation is independent of cluster connectivity. The
construction error from the ``__init__`` block triggers our cleanup, which
resets the process-global engine-singleton lock — so each test can
independently re-try the construction.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

# Skip the whole module if the native bridge isn't built into this env.
native = pytest.importorskip("dingofs_connector._dingofs_native")

# A real dingofs gflag we don't otherwise depend on. Carrying a unique value
# through Python → native → gflags is what proves the conf file was loaded.
TEST_FLAG = "cache_rpc_timeout_ms"


def _make_conf(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "lmcache-client.conf"
    p.write_text(textwrap.dedent(body).lstrip())
    return p


def _try_construct(conf_file: str) -> Exception:
    """Construct RemoteCache against an unreachable MDS; return the raised exception.

    The construction is guaranteed to fail (either at InstallFlags if the
    conf file is bad, or at StartCache because the MDS endpoint won't respond).
    For tests that care about the conf-file step, we just need to know what
    kind of error came out.
    """
    with pytest.raises(Exception) as exc_info:
        native.RemoteCache(
            mds_addrs="127.0.0.1:1",   # IANA-reserved unrouteable port
            cache_group="lmcache-test",
            conf_file=conf_file,
        )
    return exc_info.value


# --- happy path: conf file contents land in gflags --------------------------


def test_valid_conf_file_sets_gflag(tmp_path):
    conf = _make_conf(tmp_path, f"""
        # smoke conf
        --{TEST_FLAG}=12345
    """)

    err = _try_construct(str(conf))

    # InstallFlags succeeded → the flag was applied before StartCache failed.
    # StartCache failure surfaces as some MDS-related runtime error — exact
    # message varies, but it must NOT mention conf-file loading.
    assert "Failed to load dingofs gflags" not in str(err)
    assert native.get_flag(TEST_FLAG) == "12345"


def test_comments_and_blank_lines_tolerated(tmp_path):
    # dingofs's own conf-file format allows '#' comments and blank lines.
    # gflags::ReadFromFlagsFile must accept the same.
    conf = _make_conf(tmp_path, f"""
        # leading comment

        --{TEST_FLAG}=23456

        # trailing comment
    """)

    _try_construct(str(conf))
    assert native.get_flag(TEST_FLAG) == "23456"


# --- sad path: surfaces as a Python exception, not exit(1) -----------------


def test_unknown_flag_in_conf_raises(tmp_path):
    conf = _make_conf(tmp_path, """
        --this_flag_does_not_exist=anything
    """)

    err = _try_construct(str(conf))
    assert "Failed to load dingofs gflags" in str(err)


def test_missing_conf_file_raises(tmp_path):
    err = _try_construct(str(tmp_path / "does-not-exist.conf"))
    assert "Failed to load dingofs gflags" in str(err)


def test_empty_conf_path_skipped(tmp_path):
    # Empty string means "no conf file" — ReadFromFlagsFile must NOT be called.
    # Construction still fails (no MDS), but the failure must come from
    # StartCache, not from the conf-loading step.
    err = _try_construct("")
    assert "Failed to load dingofs gflags" not in str(err)
