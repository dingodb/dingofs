# SPDX-License-Identifier: Apache-2.0
"""ExistsLRU tests — capacity, recency, thread safety."""

import threading

import pytest

from dingofs_connector.exists_cache import ExistsLRU


def test_add_and_has():
    lru = ExistsLRU(capacity=4)
    lru.add("a")
    assert lru.has("a")
    assert not lru.has("b")


def test_capacity_evicts_oldest():
    lru = ExistsLRU(capacity=3)
    for k in ["a", "b", "c"]:
        lru.add(k)
    lru.add("d")  # evicts "a"
    assert not lru.has("a")
    assert all(lru.has(k) for k in ["b", "c", "d"])


def test_re_add_refreshes_recency():
    lru = ExistsLRU(capacity=3)
    for k in ["a", "b", "c"]:
        lru.add(k)
    lru.add("a")  # a is now most-recent
    lru.add("d")  # evicts "b", not "a"
    assert lru.has("a")
    assert not lru.has("b")
    assert lru.has("c") and lru.has("d")


def test_has_marks_recency():
    lru = ExistsLRU(capacity=3)
    for k in ["a", "b", "c"]:
        lru.add(k)
    assert lru.has("a")  # touches a
    lru.add("d")  # evicts "b" (oldest), not "a"
    assert lru.has("a")
    assert not lru.has("b")


def test_add_many_respects_capacity():
    lru = ExistsLRU(capacity=3)
    lru.add_many(["a", "b", "c", "d", "e"])
    assert len(lru) == 3
    assert all(lru.has(k) for k in ["c", "d", "e"])


def test_zero_capacity_is_rejected():
    with pytest.raises(ValueError):
        ExistsLRU(capacity=0)


def test_concurrent_add_has():
    # Stress test: many threads adding & probing — must not crash, must not
    # blow past capacity.
    lru = ExistsLRU(capacity=200)
    stop = threading.Event()

    def writer(prefix: str):
        i = 0
        while not stop.is_set():
            lru.add(f"{prefix}-{i}")
            i += 1

    def reader():
        while not stop.is_set():
            lru.has("anything")

    threads = (
        [threading.Thread(target=writer, args=(p,)) for p in "abcd"]
        + [threading.Thread(target=reader) for _ in range(2)]
    )
    for t in threads:
        t.start()

    # Brief stress run.
    import time
    time.sleep(0.2)
    stop.set()
    for t in threads:
        t.join(timeout=1.0)

    assert len(lru) <= 200
