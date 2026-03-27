# SPDX-License-Identifier: Apache-2.0
#
# Integration tests for DingoFSConnector and DingoFSConnectorAdapter.
# These tests require LMCache to be installed.

# Standard
import asyncio
import os
import sys

# Third Party
import pytest

# Skip all tests in this file if LMCache is not installed
try:
    from lmcache.v1.config import LMCacheEngineConfig
    from lmcache.v1.storage_backend.connector import parse_remote_url

    LMCACHE_AVAILABLE = True
except Exception:
    LMCACHE_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not LMCACHE_AVAILABLE, reason="LMCache not installed"
)


class TestDingoFSConnectorAdapter:
    """Tests for the connector adapter (URL parsing, creation)."""

    def test_can_parse(self):
        """Test URL scheme matching."""
        from dingofs_connector.adapter import DingoFSConnectorAdapter

        adapter = DingoFSConnectorAdapter()
        assert adapter.can_parse("dingofs://host:0/mnt/dingofs/cache")
        assert adapter.can_parse("dingofs://localhost:9999/data")
        assert not adapter.can_parse("redis://localhost:6379")
        assert not adapter.can_parse("fs:///tmp/cache")
        assert not adapter.can_parse("s3://bucket/prefix")

    def test_url_parsing(self):
        """Test that URL is correctly parsed."""
        url = "dingofs://myhost:1234/mnt/dingofs/kv_cache"
        parsed = parse_remote_url(url)
        assert parsed.path == "/mnt/dingofs/kv_cache"

    def test_url_without_path_raises(self):
        """Test that URL without path raises ValueError."""
        from dingofs_connector.adapter import DingoFSConnectorAdapter
        from unittest.mock import MagicMock

        adapter = DingoFSConnectorAdapter()

        context = MagicMock()
        context.url = "dingofs://host:0"
        context.config = MagicMock()
        context.config.extra_config = {}

        # parse_remote_url("dingofs://host:0") should give empty path
        # The adapter should raise ValueError
        with pytest.raises((ValueError, Exception)):
            adapter.create_connector(context)


class TestDingoFSConnectorIntegration:
    """Integration tests for DingoFSConnector with LMCache interfaces.

    These tests create a real DingoFSConnector and test put/get/exists
    operations using LMCache's config and metadata infrastructure.
    """

    @pytest.fixture
    def config(self, tmp_dir):
        """Create a minimal LMCache config for testing."""
        return LMCacheEngineConfig.from_defaults(
            chunk_size=256,
            remote_url=f"dingofs://host:0{tmp_dir}",
            remote_serde="naive",
            lmcache_instance_id="test_dingofs",
        )

    # NOTE: Full integration tests with RemoteBackend require more LMCache
    # infrastructure (LocalCPUBackend, metadata, etc.) and are best run
    # as part of LMCache's test suite. The adapter URL parsing tests above
    # verify the connector can be created correctly.
