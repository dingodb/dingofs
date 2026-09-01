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
    parser.addoption("--mds-addr", help="MDS HTTP address for quota tests")
    parser.addoption("--fs-id", type=int, help="filesystem ID for quota tests")
    parser.addoption("--root-ino", type=int, help="root inode for quota tests")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    setattr(item, f"rep_{call.when}", outcome.get_result())


@pytest.fixture(scope="session")
def mount_point(request):
    mp = request.config.getoption("--mount-point")
    assert os.path.isdir(mp), f"{mp} is not a directory"
    return mp


@pytest.fixture
def test_dir(request, mount_point):
    """Per-test directory; retain it when setup or test execution fails."""
    d = os.path.join(mount_point, f"test_{uuid.uuid4().hex[:8]}")
    os.makedirs(d)
    yield d
    reports = (getattr(request.node, f"rep_{phase}", None)
               for phase in ("setup", "call"))
    if any(report and report.failed for report in reports):
        print(f"case dir kept: {d}")
    else:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def quota_config(request):
    """Quota control-plane parameters, required only by quota tests."""
    values = {
        "mds_addr": request.config.getoption("--mds-addr"),
        "fs_id": request.config.getoption("--fs-id"),
        "root_ino": request.config.getoption("--root-ino"),
    }
    missing = [name for name, value in values.items() if value is None]
    if missing:
        pytest.skip("quota tests require " + ", ".join(
            "--" + name.replace("_", "-") for name in missing))
    return values
