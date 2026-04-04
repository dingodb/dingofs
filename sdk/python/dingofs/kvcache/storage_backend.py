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

"""DingoFS KVCache StorageBackend for LMCache (Phase 2).

Connects directly to CacheGroup nodes via bRPC, bypassing FUSE.
Requires the C++ KVCacheClient extension module (_kvcache_core).

Fallback: If _kvcache_core is not available, raises ImportError
with instructions to build it.
"""

import hashlib
import threading
from typing import Optional, Set

try:
    from dingofs.kvcache._kvcache_core import KVCacheClient as _NativeClient
    _HAS_NATIVE = True
except ImportError:
    _HAS_NATIVE = False


class DingoFSBackend:
    """LMCache StorageBackend backed by DingoFS CacheGroup (Phase 2)."""

    def __init__(self, config: dict):
        if not _HAS_NATIVE:
            raise ImportError(
                "DingoFS KVCache native module not found. "
                "Build with: cmake -DBUILD_KVCACHE_SDK=ON && make kvcache_sdk"
            )
        self._client = _NativeClient(
            config.get("mds_addrs", ""),
            config.get("model_id", 0),
        )
        self._client.start()
        self._index: Set[str] = set()
        self._lock = threading.Lock()

    def close(self) -> None:
        self._client.shutdown()
        with self._lock:
            self._index.clear()

    def put(self, key: str, data: bytes) -> None:
        hash_bytes = self._key_to_hash(key)
        self._client.put(hash_bytes, data)
        with self._lock:
            self._index.add(key)

    def get(self, key: str) -> Optional[bytes]:
        hash_bytes = self._key_to_hash(key)
        try:
            return self._client.get(hash_bytes)
        except RuntimeError:
            return None

    def contains(self, key: str) -> bool:
        with self._lock:
            if key in self._index:
                return True
        hash_bytes = self._key_to_hash(key)
        try:
            results = self._client.batch_exists([hash_bytes])
            if results and results[0]:
                with self._lock:
                    self._index.add(key)
                return True
        except RuntimeError:
            pass
        return False

    def remove(self, key: str) -> None:
        with self._lock:
            self._index.discard(key)

    @staticmethod
    def _key_to_hash(key: str) -> bytes:
        return hashlib.sha256(key.encode()).digest()
