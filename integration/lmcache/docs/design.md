# Design

## Layer Diagram

```
┌──────────────────────────────────────────────────┐
│              LMCache Integration                  │
│  DingoFSConnector / DingoFSConnectorAdapter      │
│        URL: dingofs://host:0/mnt/path            │
├──────────────────────────────────────────────────┤
│        Python NativeIOEngine                      │
│  ctypes bindings + asyncio eventfd integration   │
├──────────────────────────────────────────────────┤
│             C API (io_engine_capi.h)             │
│  Stable extern "C" ABI for any language          │
├──────────────────────────────────────────────────┤
│         C++ IOEngine (io_engine.h)               │
│  Multi-threaded worker pool, batch tiling,       │
│  eventfd-based async completion                  │
├──────────────────────────────────────────────────┤
│           POSIX File I/O (file_io.h)             │
│  O_DIRECT, atomic writes (temp + rename)         │
└──────────────────────────────────────────────────┘
```

## Design Rationale

**Hourglass pattern** (C++ → C ABI → Python):
Following the [RocksDB](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/c.h) approach, the C++ implementation is wrapped behind a stable C API. This decouples the Python bindings from C++ internals and allows bindings in any FFI-capable language.

**Multi-threaded batch tiling**:
Each batch operation is divided into tiles distributed across worker threads. Tile completion is tracked with atomic counters; the final tile signals an eventfd to wake the caller. This maps well to DingoFS's high-concurrency I/O path.

**Zero-copy I/O**:
Read and write operations work directly on caller-provided buffers. No intermediate copies are made in the C++ or Python layers.

**Atomic writes**:
Writes go to a temporary file first, followed by `fdatasync` and an atomic `rename`. This prevents partial reads on crash.

## File Mapping

| Layer | Files |
|-------|-------|
| LMCache adapter | `src/dingofs_connector/connector.py`, `src/dingofs_connector/adapter.py` |
| Python bindings | `src/dingofs_connector/native_engine.py` |
| C API | `src/io_engine/io_engine_capi.h`, `src/io_engine/io_engine_capi.cpp` |
| C++ engine | `src/io_engine/io_engine.h`, `src/io_engine/io_engine.cpp` |
| POSIX I/O | `src/io_engine/file_io.h`, `src/io_engine/file_io.cpp` |

## C API Reference

Header: [`src/io_engine/io_engine_capi.h`](../src/io_engine/io_engine_capi.h)

All functions are thread-safe. The opaque handle can be shared across threads.

### Lifecycle

```c
// Create an I/O engine. Returns NULL on failure.
io_engine_t* io_engine_create(const char* base_path, int num_workers,
                              int use_odirect, int sync_mode);

// Destroy and free all resources.
void io_engine_destroy(io_engine_t* engine);

// Graceful shutdown. Idempotent.
void io_engine_close(io_engine_t* engine);
```

### Submit Operations

All submit functions are non-blocking and return a `future_id` (0 on error).

```c
uint64_t io_engine_submit_batch_get(io_engine_t* engine,
    const char** keys, void** bufs, const size_t* lens,
    size_t count, size_t chunk_size);

uint64_t io_engine_submit_batch_set(io_engine_t* engine,
    const char** keys, const void** bufs, const size_t* lens,
    size_t count, size_t chunk_size);

uint64_t io_engine_submit_batch_exists(io_engine_t* engine,
    const char** keys, size_t count);
```

### Completions

```c
// Drain available completions. Returns count written to out.
// Pointers in io_completion_t are valid until the next drain call.
int io_engine_drain_completions(io_engine_t* engine,
    io_completion_t* out, size_t max_completions);
```

`io_completion_t` fields:

| Field | Type | Description |
|-------|------|-------------|
| `future_id` | `uint64_t` | Matches the submit return value |
| `ok` | `int` | 1 = success, 0 = failure |
| `error` | `const char*` | Error message (NULL if ok) |
| `result_bytes` | `const uint8_t*` | EXISTS results: 0/1 per key (NULL otherwise) |
| `result_len` | `size_t` | Length of `result_bytes` |

### Async Notification

```c
// Returns an eventfd that becomes readable when completions are available.
int io_engine_event_fd(const io_engine_t* engine);
```

### Error Handling

```c
// Thread-local last error. Returns NULL if no error.
const char* io_engine_last_error(void);
```

### Constants

```c
#define IO_ENGINE_SYNC_NONE   0  // No fdatasync
#define IO_ENGINE_SYNC_ALWAYS 1  // fdatasync after every write
```
