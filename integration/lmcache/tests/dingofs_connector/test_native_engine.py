# SPDX-License-Identifier: Apache-2.0

# Standard
import asyncio
import os

# Third Party
import pytest
import pytest_asyncio

# Local
from dingofs_connector.native_engine import NativeIOEngine


class TestNativeClientSync:
    """Synchronous tests for NativeIOEngine (native C++ I/O engine)."""

    def test_create_and_close(self, tmp_dir):
        """Test basic lifecycle."""
        client = NativeIOEngine(tmp_dir, num_workers=2)
        assert client.event_fd() >= 0
        client.close()

    def test_single_set_get(self, tmp_dir):
        """Test single key SET + GET round-trip."""
        client = NativeIOEngine(tmp_dir, num_workers=2)

        data = bytearray(b"hello dingofs!" * 100)
        buf = memoryview(data)

        # SET
        client.set_sync("test_key", buf)

        # GET
        read_data = bytearray(len(data))
        read_buf = memoryview(read_data)
        client.get_sync("test_key", read_buf)

        assert data == read_data
        client.close()

    def test_exists(self, tmp_dir):
        """Test EXISTS for present and absent keys."""
        client = NativeIOEngine(tmp_dir, num_workers=2)

        # Non-existent key
        assert client.exists_sync("no_such_key") is False

        # Write a key
        data = bytearray(b"x" * 512)
        client.set_sync("my_key", memoryview(data))

        # Now should exist
        assert client.exists_sync("my_key") is True

        client.close()

    def test_batch_operations(self, tmp_dir):
        """Test batch SET + GET + EXISTS."""
        client = NativeIOEngine(tmp_dir, num_workers=4)

        num_keys = 5
        data_size = 2048
        keys = [f"batch_key_{i}" for i in range(num_keys)]

        # Prepare write buffers
        write_bufs = []
        for i in range(num_keys):
            data = bytearray(bytes([i & 0xFF]) * data_size)
            write_bufs.append(data)

        # Batch SET
        client.batch_set_sync(keys, [memoryview(b) for b in write_bufs])

        # Batch EXISTS
        results = client.batch_exists_sync(keys)
        assert len(results) == num_keys
        assert all(results)

        # Batch GET
        read_bufs = [bytearray(data_size) for _ in range(num_keys)]
        client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])

        for i in range(num_keys):
            assert write_bufs[i] == read_bufs[i], f"Data mismatch for key {i}"

        client.close()

    def test_large_data(self, tmp_dir):
        """Test with 1MB data (typical KV cache chunk size)."""
        client = NativeIOEngine(tmp_dir, num_workers=4)

        data_size = 1024 * 1024  # 1 MB
        data = bytearray(os.urandom(data_size))

        client.set_sync("large_key", memoryview(data))

        read_data = bytearray(data_size)
        client.get_sync("large_key", memoryview(read_data))

        assert data == read_data
        client.close()

    def test_error_on_get_nonexistent(self, tmp_dir):
        """Test that GET on non-existent key raises an error."""
        client = NativeIOEngine(tmp_dir, num_workers=2)

        data = bytearray(1024)
        with pytest.raises(RuntimeError):
            client.get_sync("nonexistent", memoryview(data))

        client.close()

    def test_multiple_workers(self, tmp_dir):
        """Test with many workers for concurrency."""
        client = NativeIOEngine(tmp_dir, num_workers=8)

        num_keys = 20
        data_size = 4096
        keys = [f"worker_key_{i}" for i in range(num_keys)]
        write_bufs = [bytearray(os.urandom(data_size)) for _ in range(num_keys)]

        client.batch_set_sync(keys, [memoryview(b) for b in write_bufs])

        read_bufs = [bytearray(data_size) for _ in range(num_keys)]
        client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])

        for i in range(num_keys):
            assert write_bufs[i] == read_bufs[i]

        client.close()


class TestNativeClientAsync:
    """Async tests for NativeIOEngine (native C++ I/O engine)."""

    @pytest.mark.asyncio
    async def test_async_set_get(self, tmp_dir):
        """Test async SET + GET."""
        loop = asyncio.get_event_loop()
        client = NativeIOEngine(tmp_dir, num_workers=2, loop=loop)

        data = bytearray(b"async hello!" * 100)
        await client.set("async_key", memoryview(data))

        read_data = bytearray(len(data))
        await client.get("async_key", memoryview(read_data))

        assert data == read_data
        client.close()

    @pytest.mark.asyncio
    async def test_async_exists(self, tmp_dir):
        """Test async EXISTS."""
        loop = asyncio.get_event_loop()
        client = NativeIOEngine(tmp_dir, num_workers=2, loop=loop)

        assert await client.exists("no_key") is False

        data = bytearray(b"x" * 512)
        await client.set("yes_key", memoryview(data))

        assert await client.exists("yes_key") is True
        client.close()

    @pytest.mark.asyncio
    async def test_async_batch(self, tmp_dir):
        """Test async batch operations."""
        loop = asyncio.get_event_loop()
        client = NativeIOEngine(tmp_dir, num_workers=4, loop=loop)

        keys = [f"abatch_{i}" for i in range(5)]
        data_size = 1024
        write_bufs = [bytearray(os.urandom(data_size)) for _ in keys]

        await client.batch_set(keys, [memoryview(b) for b in write_bufs])

        results = await client.batch_exists(keys)
        assert all(results)

        read_bufs = [bytearray(data_size) for _ in keys]
        await client.batch_get(keys, [memoryview(b) for b in read_bufs])

        for i, key in enumerate(keys):
            assert write_bufs[i] == read_bufs[i], f"Mismatch for {key}"

        client.close()
