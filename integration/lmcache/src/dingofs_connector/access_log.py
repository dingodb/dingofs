# SPDX-License-Identifier: Apache-2.0
"""Per-operation access log for the dingofs connector.

Mirrors the format used by dingofs's vfs access log
(src/client/vfs/access_log.h), one line per operation:

    <op>(<args>) : <result> <duration_seconds>

e.g.::

    batched_get(20 keys) : hits=20/20 <0.007234>
    native.batch_set(1 keys, 65536 bytes) : ok <0.000821>
    get(smoke-client@1@0@cafebabe@half) : not_found <0.001012>
    put(smoke-client@1@0@cafebabe@half, 4194304) : FAIL RuntimeError: IoError <0.001230>

Off by default. Controlled by environment variables (read once at import):

    DINGOFS_ACCESS_LOG_ENABLED       "1" to turn on (default off)
    DINGOFS_ACCESS_LOG_PATH          file path; empty = stderr (default empty)
    DINGOFS_ACCESS_LOG_THRESHOLD_US  only log ops whose wall time exceeds this
                                     (default 0 = log every call)

Performance:
  - Disabled: ~100 ns/call. A frozen _NoopLog singleton is returned, so the
    context-manager protocol skips contextmanager machinery / timer / arg
    formatting entirely.
  - Enabled: file writes go through a logging.handlers.QueueHandler with a
    background QueueListener thread, so foreground emit cost is ~3 µs
    (enqueue only) — no synchronous write/flush in the hot path.
"""

from __future__ import annotations

import atexit
import logging
import logging.handlers
import os
import queue
import sys
import time
from typing import Any, Callable, Optional


# Read once at import; flipping env mid-process is not supported.
_ENABLED = os.environ.get(
    "DINGOFS_ACCESS_LOG_ENABLED", "0"
) in ("1", "true", "yes", "on")
_LOG_PATH = os.environ.get("DINGOFS_ACCESS_LOG_PATH", "").strip()
try:
    _THRESHOLD_US = int(os.environ.get("DINGOFS_ACCESS_LOG_THRESHOLD_US", "0"))
except ValueError:
    _THRESHOLD_US = 0


def _build_logger() -> Optional[logging.Logger]:
    """Build a non-blocking access logger.

    Foreground emit only enqueues a LogRecord; a background QueueListener
    thread does the actual write/flush. This keeps the hot path under a few
    microseconds even at thousands of ops per second.
    """
    if not _ENABLED:
        return None
    log = logging.getLogger("dingofs.access")
    log.setLevel(logging.INFO)
    log.propagate = False  # keep out of root logger / LMCache's stack
    if log.handlers:
        return log  # already configured (e.g. on re-import)

    if _LOG_PATH:
        try:
            sink: logging.Handler = logging.FileHandler(_LOG_PATH, mode="a")
        except OSError as exc:
            sys.stderr.write(
                f"[dingofs.access] cannot open {_LOG_PATH!r}: {exc}; "
                f"falling back to stderr\n"
            )
            sink = logging.StreamHandler(sys.stderr)
    else:
        sink = logging.StreamHandler(sys.stderr)
    sink.setFormatter(
        logging.Formatter("%(asctime)s.%(msecs)03d %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )

    # Unbounded queue: enqueueing never blocks; if the listener can't keep up
    # we'd rather grow memory than stall the hot path. In practice the queue
    # stays empty because emit is ~10 µs and ops are ~ms.
    q: "queue.Queue[logging.LogRecord]" = queue.Queue(-1)
    listener = logging.handlers.QueueListener(q, sink, respect_handler_level=False)
    listener.start()
    atexit.register(listener.stop)

    log.addHandler(logging.handlers.QueueHandler(q))
    return log


_logger: Optional[logging.Logger] = _build_logger()


def is_enabled() -> bool:
    return _ENABLED


# ---------------------------------------------------------------------------
# Real (enabled) context manager — keeps a timer and emits on exit.
# ---------------------------------------------------------------------------

class _RealLog:
    __slots__ = ("_op", "_args_fn", "_start", "result")

    def __init__(self, op: str, args_fn: Callable[[], str]) -> None:
        self._op = op
        self._args_fn = args_fn
        self._start = 0.0
        self.result: str = "OK"

    def __enter__(self) -> "_RealLog":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration = time.perf_counter() - self._start
        if exc_type is not None:
            self.result = f"FAIL {exc_type.__name__}: {exc_val}"
        if duration * 1_000_000 >= _THRESHOLD_US and _logger is not None:
            _logger.info("%s(%s) : %s <%.6f>",
                         self._op, self._args_fn(), self.result, duration)
        return False  # don't swallow exceptions


# ---------------------------------------------------------------------------
# Noop singleton — what disabled access_log returns. ~100 ns / call.
# ---------------------------------------------------------------------------

class _NoopLog:
    """Frozen singleton used when the access log is disabled.

    Implements just enough of the context-manager protocol to be a drop-in
    for _RealLog. Has a writable `result` attribute that nobody reads.
    """
    __slots__ = ()

    result: Any = "OK"  # class attribute; .result = ... below is a no-op

    def __enter__(self) -> "_NoopLog":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def __setattr__(self, name: str, value: Any) -> None:
        # Allow `r.result = "..."` from callers without raising; just drop it.
        pass


_NOOP = _NoopLog()


# ---------------------------------------------------------------------------
# Public entry: cheap singleton when disabled, real timer when enabled.
# ---------------------------------------------------------------------------

if _ENABLED:
    def access_log(op: str, args_fn: Callable[[], str] = lambda: ""):
        return _RealLog(op, args_fn)
else:
    def access_log(op: str = "",
                   args_fn: Callable[[], str] = lambda: ""):
        return _NOOP
