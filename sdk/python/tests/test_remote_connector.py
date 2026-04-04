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

"""Unit tests for DingoFSRemoteConnector."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from dingofs.kvcache.remote_connector import DingoFSRemoteConnector


@pytest.fixture
def connector(tmp_path):
    """Create a DingoFSRemoteConnector backed by a temporary directory."""
    c = DingoFSRemoteConnector(
        {"mount_point": str(tmp_path), "model_name": "test-model"}
    )
    yield c
    c.close()


class TestBasicCRUD:
    """Basic create / read / update / delete operations."""

    def test_put_and_get(self, connector):
        connector.put("k1", b"hello")
        assert connector.get("k1") == b"hello"

    def test_get_nonexistent(self, connector):
        assert connector.get("no-such-key") is None

    def test_exists_after_put(self, connector):
        connector.put("k1", b"data")
        assert connector.exists("k1") is True

    def test_exists_nonexistent(self, connector):
        assert connector.exists("no-such-key") is False

    def test_delete_existing(self, connector):
        connector.put("k1", b"data")
        connector.delete("k1")
        assert connector.get("k1") is None
        assert connector.exists("k1") is False

    def test_delete_nonexistent(self, connector):
        connector.delete("no-such-key")  # should not raise

    def test_put_overwrite(self, connector):
        connector.put("k1", b"first")
        connector.put("k1", b"second")
        assert connector.get("k1") == b"second"

    def test_put_empty_data(self, connector):
        connector.put("k1", b"")
        assert connector.get("k1") == b""


class TestLocalIndex:
    """Tests for the in-memory _known_keys index."""

    def test_exists_uses_local_index(self, connector):
        connector.put("k1", b"data")
        # Remove the file behind the connector's back.
        os.remove(connector._key_to_path("k1"))
        # The in-memory index still knows about the key.
        assert connector.exists("k1") is True

    def test_exists_fallback_to_disk(self, connector):
        # Manually create a file without going through put().
        path = connector._key_to_path("manual-key")
        with open(path, "wb") as fh:
            fh.write(b"payload")
        # exists() should discover it on disk and add it to the index.
        assert connector.exists("manual-key") is True
        assert "manual-key" in connector._known_keys


class TestThreadSafety:
    """Concurrent access must not corrupt state."""

    def test_concurrent_put_and_exists(self, connector):
        num_threads = 10
        keys_per_thread = 100

        def worker(thread_id):
            for i in range(keys_per_thread):
                key = f"t{thread_id}-k{i}"
                connector.put(key, b"v")

        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            futures = [pool.submit(worker, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()  # propagate exceptions

        for t in range(num_threads):
            for i in range(keys_per_thread):
                assert connector.exists(f"t{t}-k{i}") is True


class TestDirectoryCreation:
    """Filesystem setup and teardown behaviour."""

    def test_base_dir_created_on_init(self, connector):
        assert os.path.isdir(connector._base_dir)

    def test_close_clears_index(self, connector):
        connector.put("a", b"1")
        connector.put("b", b"2")
        connector.close()
        assert len(connector._known_keys) == 0
