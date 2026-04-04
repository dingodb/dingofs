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

"""Remote connector that stores KV cache entries as files on DingoFS."""

import os
import threading
from typing import Optional, Set


class DingoFSRemoteConnector:
    """Stores KV cache blobs as files under a DingoFS mount point."""

    def __init__(self, config: dict) -> None:
        """Initialize the connector.

        Args:
            config: Dictionary with ``mount_point`` and ``model_name``.
        """
        mount_point: str = config["mount_point"]
        model_name: str = config["model_name"]
        self._base_dir: str = os.path.join(mount_point, "kvcache", model_name)
        self._known_keys: Set[str] = set()
        self._lock = threading.Lock()
        os.makedirs(self._base_dir, exist_ok=True)

    def _key_to_path(self, key: str) -> str:
        """Return the absolute file path for *key*."""
        return os.path.join(self._base_dir, key)

    def exists(self, key: str) -> bool:
        """Check whether *key* exists.

        Uses an in-memory set as a fast path before falling back to the
        filesystem.
        """
        with self._lock:
            if key in self._known_keys:
                return True
        if os.path.exists(self._key_to_path(key)):
            with self._lock:
                self._known_keys.add(key)
            return True
        return False

    def put(self, key: str, data: bytes) -> None:
        """Write *data* to the file identified by *key*."""
        with open(self._key_to_path(key), "wb") as fh:
            fh.write(data)
        with self._lock:
            self._known_keys.add(key)

    def get(self, key: str) -> Optional[bytes]:
        """Read and return the data for *key*, or ``None`` if missing."""
        try:
            with open(self._key_to_path(key), "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            return None

    def delete(self, key: str) -> None:
        """Remove the file for *key*, ignoring missing files."""
        try:
            os.remove(self._key_to_path(key))
        except FileNotFoundError:
            pass
        with self._lock:
            self._known_keys.discard(key)

    def close(self) -> None:
        """Release resources."""
        with self._lock:
            self._known_keys.clear()
