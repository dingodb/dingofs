# SPDX-License-Identifier: Apache-2.0

# Standard
from typing import Any, Dict, List, Optional, Tuple, Union
import asyncio
import concurrent.futures
import ctypes
import ctypes.util
import os
import pathlib

# Sync mode constants (must match io_engine_capi.h)
SYNC_NONE = 0
SYNC_ALWAYS = 1


# Completion struct matching io_completion_t in io_engine_capi.h
class _IOCompletion(ctypes.Structure):
    _fields_ = [
        ("future_id", ctypes.c_uint64),
        ("ok", ctypes.c_int),
        ("error", ctypes.c_char_p),
        ("result_bytes", ctypes.POINTER(ctypes.c_uint8)),
        ("result_len", ctypes.c_size_t),
    ]


def _find_library() -> str:
    """Find the libdingofs_connector.so shared library.

    Search order:
    1. Bundled in package directory (wheel install)
    2. DINGOFS_CONNECTOR_LIB environment variable (explicit override)
    3. build/ directory relative to project root (dev workflow)
    4. System library path via ldconfig

    Returns:
        Path to the shared library.

    Raises:
        RuntimeError: If the library cannot be found.
    """
    LIB_NAME = "libdingofs_connector.so"

    # 1. Bundled in the same directory as this Python file (wheel install)
    pkg_dir = pathlib.Path(__file__).resolve().parent
    bundled = pkg_dir / LIB_NAME
    if bundled.is_file():
        return str(bundled)

    # 2. Explicit env var override
    env_path = os.environ.get("DINGOFS_CONNECTOR_LIB")
    if env_path and os.path.isfile(env_path):
        return env_path

    # 3. Development build directory
    root_dir = pkg_dir.parent.parent
    build_path = root_dir / "build" / LIB_NAME
    if build_path.is_file():
        return str(build_path)

    # 4. System library path
    sys_path = ctypes.util.find_library("dingofs_connector")
    if sys_path:
        return sys_path

    raise RuntimeError(
        "Cannot find libdingofs_connector.so. "
        "Install with: pip install dingofs-connector"
    )


def _load_library() -> ctypes.CDLL:
    """Load the shared library and define function signatures."""
    lib_path = _find_library()
    lib = ctypes.CDLL(lib_path)

    # io_engine_create
    lib.io_engine_create.argtypes = [
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
    ]
    lib.io_engine_create.restype = ctypes.c_void_p

    # io_engine_destroy
    lib.io_engine_destroy.argtypes = [ctypes.c_void_p]
    lib.io_engine_destroy.restype = None

    # io_engine_event_fd
    lib.io_engine_event_fd.argtypes = [ctypes.c_void_p]
    lib.io_engine_event_fd.restype = ctypes.c_int

    # io_engine_submit_batch_get
    lib.io_engine_submit_batch_get.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_size_t,
        ctypes.c_size_t,
    ]
    lib.io_engine_submit_batch_get.restype = ctypes.c_uint64

    # io_engine_submit_batch_set
    lib.io_engine_submit_batch_set.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_size_t,
        ctypes.c_size_t,
    ]
    lib.io_engine_submit_batch_set.restype = ctypes.c_uint64

    # io_engine_submit_batch_exists
    lib.io_engine_submit_batch_exists.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_size_t,
    ]
    lib.io_engine_submit_batch_exists.restype = ctypes.c_uint64

    # io_engine_drain_completions
    lib.io_engine_drain_completions.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_IOCompletion),
        ctypes.c_size_t,
    ]
    lib.io_engine_drain_completions.restype = ctypes.c_int

    # io_engine_close
    lib.io_engine_close.argtypes = [ctypes.c_void_p]
    lib.io_engine_close.restype = None

    # io_engine_last_error
    lib.io_engine_last_error.argtypes = []
    lib.io_engine_last_error.restype = ctypes.c_char_p

    return lib


# Singleton library handle
_lib: Optional[ctypes.CDLL] = None


def _get_lib() -> ctypes.CDLL:
    global _lib
    if _lib is None:
        _lib = _load_library()
    return _lib


class NativeIOEngine:
    """Python wrapper around the DingoFS I/O engine C library.

    Provides both sync and async interfaces with eventfd-based completion
    notification for integrating with Python asyncio event loops.

    Args:
        base_path: Directory where cache files are stored.
        num_workers: Number of I/O worker threads.
        use_odirect: Whether to use O_DIRECT for file I/O.
        sync_mode: Controls fdatasync behavior (SYNC_NONE or SYNC_ALWAYS).
        loop: Asyncio event loop (uses running loop if None).
    """

    # Max completions to drain per call
    MAX_DRAIN = 256

    def __init__(
        self,
        base_path: str,
        num_workers: int = 8,
        use_odirect: bool = False,
        sync_mode: int = SYNC_ALWAYS,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self._lib = _get_lib()

        self._handle = self._lib.io_engine_create(
            base_path.encode("utf-8"),
            num_workers,
            1 if use_odirect else 0,
            sync_mode,
        )
        if not self._handle:
            err = self._lib.io_engine_last_error()
            msg = err.decode("utf-8") if err else "unknown error"
            raise RuntimeError(f"Failed to create DingoFS IOEngine: {msg}")

        self._fd = int(self._lib.io_engine_event_fd(self._handle))
        self._closed = False

        # Pre-allocate completion buffer
        self._comp_buf = (_IOCompletion * self.MAX_DRAIN)()

        # Pending futures: future_id -> (Future, op_type, keepalive_refs)
        self._pending: Dict[
            int,
            Tuple[
                Union[asyncio.Future, concurrent.futures.Future],
                str,
                Tuple[Any, ...],
            ],
        ] = {}

        # Asyncio integration
        self._loop = loop
        if self._loop is not None:
            self._loop.add_reader(self._fd, self._on_ready)

    def event_fd(self) -> int:
        """Get the eventfd file descriptor for async notification."""
        return self._fd

    # ------------------------------------------------------------------
    # Asyncio event loop callback
    # ------------------------------------------------------------------

    def _on_ready(self) -> None:
        """Called by the event loop when completions are available."""
        if self._closed:
            return
        try:
            while True:
                items = self._drain_raw()
                if not items:
                    return
                for future_id, ok, error, result_bools in items:
                    entry = self._pending.pop(future_id, None)
                    if entry is None:
                        continue
                    fut, op, _keepalive = entry
                    if fut.done():
                        continue
                    if ok:
                        if op == "exists":
                            if result_bools and len(result_bools) > 0:
                                fut.set_result(bool(result_bools[0]))
                            else:
                                fut.set_result(False)
                        elif op == "batch_exists":
                            fut.set_result(
                                list(result_bools) if result_bools else []
                            )
                        else:
                            fut.set_result(None)
                    else:
                        fut.set_exception(RuntimeError(str(error)))
        except Exception as e:
            self._fail_all(RuntimeError(f"drain_completions failed: {e}"))
            self._shutdown_native(best_effort=True)

    def _drain_raw(
        self,
    ) -> List[Tuple[int, bool, str, Optional[List[bool]]]]:
        """Drain completions from C library. Returns list of tuples."""
        n = self._lib.io_engine_drain_completions(
            self._handle, self._comp_buf, self.MAX_DRAIN
        )
        if n <= 0:
            return []

        results = []
        for i in range(n):
            c = self._comp_buf[i]
            fid = int(c.future_id)
            ok = bool(c.ok)
            error = c.error.decode("utf-8") if c.error else ""

            bools: Optional[List[bool]] = None
            if c.result_bytes and c.result_len > 0:
                bools = [bool(c.result_bytes[j]) for j in range(c.result_len)]

            results.append((fid, ok, error, bools))
        return results

    def _fail_all(self, exc: Exception) -> None:
        for _fid, (fut, _, _keepalive) in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    def _shutdown_native(self, best_effort: bool = False) -> None:
        try:
            self._closed = True
            if self._loop is not None:
                self._loop.remove_reader(self._fd)
        except Exception:
            if not best_effort:
                raise

    # ------------------------------------------------------------------
    # Future registration
    # ------------------------------------------------------------------

    def _register_future_async(
        self, op: str, future_id: int, keepalive: Tuple[Any, ...] = ()
    ) -> asyncio.Future:
        assert self._loop is not None
        fut = self._loop.create_future()
        self._pending[future_id] = (fut, op, keepalive)
        return fut

    def _register_future_sync(
        self, op: str, future_id: int, keepalive: Tuple[Any, ...] = ()
    ) -> concurrent.futures.Future:
        fut: concurrent.futures.Future = concurrent.futures.Future()
        self._pending[future_id] = (fut, op, keepalive)
        return fut

    # ------------------------------------------------------------------
    # Helper: build C arrays from Python lists
    # ------------------------------------------------------------------

    @staticmethod
    def _build_key_array(
        keys: List[str],
    ) -> Tuple[ctypes.Array, List[bytes]]:
        """Build a C array of char* from Python strings.

        Returns the ctypes array and the encoded bytes list (for keepalive).
        """
        encoded = [k.encode("utf-8") for k in keys]
        arr = (ctypes.c_char_p * len(keys))(*encoded)
        return arr, encoded

    @staticmethod
    def _build_buf_arrays(
        bufs: List[memoryview],
    ) -> Tuple[ctypes.Array, ctypes.Array]:
        """Build C arrays of void* and size_t from memoryviews."""
        n = len(bufs)
        ptr_arr = (ctypes.c_void_p * n)()
        len_arr = (ctypes.c_size_t * n)()
        for i, mv in enumerate(bufs):
            buf = (ctypes.c_char * len(mv)).from_buffer(mv)
            ptr_arr[i] = ctypes.cast(buf, ctypes.c_void_p).value
            len_arr[i] = len(mv)
        return ptr_arr, len_arr

    # ------------------------------------------------------------------
    # Async API
    # ------------------------------------------------------------------

    async def get(self, key: str, buf: memoryview) -> None:
        """Async GET: read value into buf."""
        return await self.batch_get([key], [buf])

    async def set(self, key: str, buf: memoryview) -> None:
        """Async SET: write buf as value."""
        return await self.batch_set([key], [buf])

    async def exists(self, key: str) -> bool:
        """Async EXISTS: check if key exists."""
        results = await self.batch_exists([key])
        return results[0]

    async def batch_get(
        self, keys: List[str], bufs: List[memoryview]
    ) -> None:
        """Async batch GET."""
        if len(keys) != len(bufs):
            raise ValueError("keys and bufs length mismatch")
        key_arr, encoded = self._build_key_array(keys)
        ptr_arr, len_arr = self._build_buf_arrays(bufs)
        chunk_size = len(bufs[0]) if bufs else 0

        fid = self._lib.io_engine_submit_batch_get(
            self._handle, key_arr, ptr_arr, len_arr, len(keys), chunk_size
        )
        if fid == 0:
            err = self._lib.io_engine_last_error()
            raise RuntimeError(
                err.decode("utf-8") if err else "submit_batch_get failed"
            )

        fut = self._register_future_async(
            "batch_get", fid, (key_arr, encoded, ptr_arr, len_arr, bufs)
        )
        return await fut

    async def batch_set(
        self, keys: List[str], bufs: List[memoryview]
    ) -> None:
        """Async batch SET."""
        if len(keys) != len(bufs):
            raise ValueError("keys and bufs length mismatch")
        key_arr, encoded = self._build_key_array(keys)
        ptr_arr, len_arr = self._build_buf_arrays(bufs)
        chunk_size = len(bufs[0]) if bufs else 0

        fid = self._lib.io_engine_submit_batch_set(
            self._handle, key_arr, ptr_arr, len_arr, len(keys), chunk_size
        )
        if fid == 0:
            err = self._lib.io_engine_last_error()
            raise RuntimeError(
                err.decode("utf-8") if err else "submit_batch_set failed"
            )

        fut = self._register_future_async(
            "batch_set", fid, (key_arr, encoded, ptr_arr, len_arr, bufs)
        )
        return await fut

    async def batch_exists(self, keys: List[str]) -> List[bool]:
        """Async batch EXISTS."""
        key_arr, encoded = self._build_key_array(keys)

        fid = self._lib.io_engine_submit_batch_exists(
            self._handle, key_arr, len(keys)
        )
        if fid == 0:
            err = self._lib.io_engine_last_error()
            raise RuntimeError(
                err.decode("utf-8") if err else "submit_batch_exists failed"
            )

        fut = self._register_future_async(
            "batch_exists", fid, (key_arr, encoded)
        )
        return await fut

    # ------------------------------------------------------------------
    # Sync API (blocks until completion via eventfd polling)
    # ------------------------------------------------------------------

    def get_sync(self, key: str, buf: memoryview) -> None:
        """Sync GET."""
        self.batch_get_sync([key], [buf])

    def set_sync(self, key: str, buf: memoryview) -> None:
        """Sync SET."""
        self.batch_set_sync([key], [buf])

    def exists_sync(self, key: str) -> bool:
        """Sync EXISTS."""
        results = self.batch_exists_sync([key])
        return results[0]

    def batch_get_sync(
        self, keys: List[str], bufs: List[memoryview]
    ) -> None:
        """Sync batch GET."""
        if len(keys) != len(bufs):
            raise ValueError("keys and bufs length mismatch")
        key_arr, encoded = self._build_key_array(keys)
        ptr_arr, len_arr = self._build_buf_arrays(bufs)
        chunk_size = len(bufs[0]) if bufs else 0

        fid = self._lib.io_engine_submit_batch_get(
            self._handle, key_arr, ptr_arr, len_arr, len(keys), chunk_size
        )
        if fid == 0:
            err = self._lib.io_engine_last_error()
            raise RuntimeError(
                err.decode("utf-8") if err else "submit_batch_get failed"
            )

        self._wait_for_completion(fid)

    def batch_set_sync(
        self, keys: List[str], bufs: List[memoryview]
    ) -> None:
        """Sync batch SET."""
        if len(keys) != len(bufs):
            raise ValueError("keys and bufs length mismatch")
        key_arr, encoded = self._build_key_array(keys)
        ptr_arr, len_arr = self._build_buf_arrays(bufs)
        chunk_size = len(bufs[0]) if bufs else 0

        fid = self._lib.io_engine_submit_batch_set(
            self._handle, key_arr, ptr_arr, len_arr, len(keys), chunk_size
        )
        if fid == 0:
            err = self._lib.io_engine_last_error()
            raise RuntimeError(
                err.decode("utf-8") if err else "submit_batch_set failed"
            )

        self._wait_for_completion(fid)

    def batch_exists_sync(self, keys: List[str]) -> List[bool]:
        """Sync batch EXISTS."""
        key_arr, encoded = self._build_key_array(keys)

        fid = self._lib.io_engine_submit_batch_exists(
            self._handle, key_arr, len(keys)
        )
        if fid == 0:
            err = self._lib.io_engine_last_error()
            raise RuntimeError(
                err.decode("utf-8") if err else "submit_batch_exists failed"
            )

        return self._wait_for_completion(fid, is_exists=True)

    def _wait_for_completion(
        self, target_fid: int, is_exists: bool = False
    ) -> Any:
        """Block until the target future_id completes (for sync API).

        Returns None for GET/SET, or List[bool] for EXISTS.
        """
        import select

        while True:
            r, _, _ = select.select([self._fd], [], [], 5.0)
            if not r:
                continue  # Timeout, retry

            items = self._drain_raw()
            for fid, ok, error, bools in items:
                if fid == target_fid:
                    if not ok:
                        raise RuntimeError(error)
                    return (bools if bools else []) if is_exists else None
                # Resolve other pending futures if any
                entry = self._pending.pop(fid, None)
                if entry:
                    fut, _, _ = entry
                    if not fut.done():
                        if ok:
                            fut.set_result(None)
                        else:
                            fut.set_exception(RuntimeError(error))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the engine and release resources."""
        if not self._closed:
            self._shutdown_native(best_effort=True)
            self._fail_all(RuntimeError("Engine closed"))
            self._lib.io_engine_destroy(self._handle)
            self._handle = None

    def __del__(self) -> None:
        if hasattr(self, "_handle") and self._handle and not self._closed:
            try:
                self._lib.io_engine_destroy(self._handle)
            except Exception:
                pass
