# SPDX-License-Identifier: Apache-2.0
"""Type stubs for the C++ extension. See ``src/native/native_cache_engine.h``."""

from typing import List, Optional, Tuple

class NativeCacheEngine:
    def __init__(
        self,
        mds_addrs: List[str],
        group_name: str,
        fs_id: int,
        num_workers: int = 16,
        request_timeout_ms: int = 5000,
    ) -> None: ...
    @property
    def service_version(self) -> int: ...
    def event_fd(self) -> int: ...
    def submit_batch_get(
        self, keys: List[str], memoryviews: List[memoryview]
    ) -> int: ...
    def submit_batch_set(
        self, keys: List[str], memoryviews: List[memoryview]
    ) -> int: ...
    def submit_batch_exists(self, keys: List[str]) -> int: ...
    def drain_completions(
        self,
    ) -> List[Tuple[int, bool, str, Optional[List[bool]]]]: ...
    def close(self) -> None: ...
