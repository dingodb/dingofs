# SPDX-License-Identifier: Apache-2.0
"""Per-process LRU set short-circuiting Exists() lookups.

Rationale: LMCache's batched_async_contains is on the prefetch hot path. The
overwhelming common case is "I just put this key, now I'm asking does it
exist?" — answering yes from memory avoids a remote round-trip.

Tolerates false positives (server evicts a key we know about; the subsequent
Get returns NotFound and LMCache handles it). Does NOT tolerate false
negatives — on miss we must always check remotely.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Iterable

__all__ = ["ExistsLRU"]


class ExistsLRU:
    def __init__(self, capacity: int = 1_000_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._entries: "OrderedDict[str, None]" = OrderedDict()
        self._lock = threading.Lock()

    def _touch_unlocked(self, key: str) -> None:
        if key in self._entries:
            self._entries.move_to_end(key)
            return
        self._entries[key] = None
        if len(self._entries) > self._capacity:
            self._entries.popitem(last=False)

    def add(self, key: str) -> None:
        with self._lock:
            self._touch_unlocked(key)

    def add_many(self, keys: Iterable[str]) -> None:
        with self._lock:
            for key in keys:
                self._touch_unlocked(key)

    def has(self, key: str) -> bool:
        with self._lock:
            if key not in self._entries:
                return False
            self._entries.move_to_end(key)
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
