# SPDX-License-Identifier: Apache-2.0

# Standard
import asyncio
import os
import shutil
import tempfile

# Third Party
import pytest


@pytest.fixture
def tmp_dir():
    """Create a temporary directory for test data."""
    d = tempfile.mkdtemp(prefix="dingofs_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
