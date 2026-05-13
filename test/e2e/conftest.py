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

"""Shared pytest fixtures for the DingoFS end-to-end suite.

Tests assume a running dingo-client with a mountpoint reachable from the
local filesystem. Pass the mountpoint via `--mount-point=<path>`.
"""

import os
import shutil
import uuid

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--mount-point",
        required=True,
        help="FUSE mount point path (e.g. /home/me/mounts/claude-mount/<inst>)",
    )


@pytest.fixture(scope="session")
def mount_point(request):
    mp = request.config.getoption("--mount-point")
    assert os.path.isdir(mp), f"{mp} is not a directory"
    return mp


@pytest.fixture
def test_dir(mount_point):
    """Per-test isolated subdir under the mountpoint; auto-removed at teardown."""
    d = os.path.join(mount_point, f"test_{uuid.uuid4().hex[:8]}")
    os.makedirs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)
