# Copyright 2026 DingoDB. All rights reserved.

"""DingoFS statvfs(2) + quota tracking behavior.

Exercises:
  - Basic statvfs fields (f_bsize, f_blocks, f_namemax) are valid.
  - f_bavail (available blocks) is monotonically non-increasing after a
    write — exposes drifts in DingoFS quota accounting on the data path,
    which would otherwise break tools that rely on `df` to gate usage.

works on: Local + MDS.
"""

import os

import pytest

pytestmark = pytest.mark.smoke


def test_statvfs_basic(mount_point):
    """statvfs should return valid filesystem info."""
    st = os.statvfs(mount_point)
    assert st.f_bsize > 0
    assert st.f_blocks > 0
    assert st.f_namemax > 0


def test_statvfs_free_decreases_after_write(test_dir, mount_point):
    """Available blocks should not increase after writing data."""
    st_before = os.statvfs(mount_point)

    # Write 1MB
    path = os.path.join(test_dir, "fill")
    with open(path, "wb") as f:
        f.write(os.urandom(1024 * 1024))

    st_after = os.statvfs(mount_point)
    # available should not increase (equal is ok due to block alignment)
    assert st_after.f_bavail <= st_before.f_bavail
