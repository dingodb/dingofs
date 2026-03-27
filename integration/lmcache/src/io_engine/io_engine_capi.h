// SPDX-License-Identifier: Apache-2.0
//
// DingoFS I/O Engine - Public C API
//
// A high-performance, multi-threaded file I/O engine for storing LLM KV
// cache on DingoFS. Uses POSIX I/O with optional O_DIRECT, a worker thread
// pool with batch tiling, and eventfd-based async completion signaling.
//
// Thread safety: All functions are thread-safe. The engine can be shared
// across multiple threads.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

// Opaque engine handle
typedef struct io_engine_t io_engine_t;

// Completion result returned by io_engine_drain_completions.
typedef struct {
  uint64_t future_id;
  int ok;               // 1 = success, 0 = failure
  const char* error;    // Error message (NULL if ok). Valid until next drain.
  const uint8_t* result_bytes;  // For EXISTS: array of 0/1. NULL otherwise.
  size_t result_len;            // Length of result_bytes array.
} io_completion_t;

// Sync mode for file writes.
//   0 = NONE: no fdatasync (rely on filesystem guarantees)
//   1 = ALWAYS: fdatasync after every write (default, ensures durability)
#define IO_ENGINE_SYNC_NONE 0
#define IO_ENGINE_SYNC_ALWAYS 1

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Create an I/O engine.
//
// Args:
//   base_path: Directory where cache files are stored (created if needed).
//   num_workers: Number of I/O worker threads (recommend 4-16).
//   use_odirect: If non-zero, use O_DIRECT for file I/O (buffers must be
//                block-aligned).
//   sync_mode: Controls fdatasync behavior (IO_ENGINE_SYNC_NONE or
//              IO_ENGINE_SYNC_ALWAYS).
//
// Returns:
//   Engine handle, or NULL on failure (check io_engine_last_error).
io_engine_t* io_engine_create(const char* base_path, int num_workers,
                              int use_odirect, int sync_mode);

// Destroy an engine and free all resources.
// The engine must be closed first (or this will close it).
void io_engine_destroy(io_engine_t* engine);

// ---------------------------------------------------------------------------
// Event notification
// ---------------------------------------------------------------------------

// Get the eventfd file descriptor for async notification.
// Becomes readable when completions are available.
int io_engine_event_fd(const io_engine_t* engine);

// ---------------------------------------------------------------------------
// Submit operations (non-blocking, return future_id)
// ---------------------------------------------------------------------------

// Submit a batch GET: read values for keys into provided buffers.
//
// Args:
//   keys: Array of key strings (count elements).
//   bufs: Array of buffer pointers to read into (count elements).
//   lens: Array of buffer sizes (count elements).
//   count: Number of keys/buffers.
//   chunk_size: Expected size of each value.
//
// Returns: future_id for this batch, or 0 on error.
uint64_t io_engine_submit_batch_get(io_engine_t* engine, const char** keys,
                                    void** bufs, const size_t* lens,
                                    size_t count, size_t chunk_size);

// Submit a batch SET: write buffers as values for keys.
//
// Args:
//   keys: Array of key strings (count elements).
//   bufs: Array of buffer pointers to write from (count elements).
//   lens: Array of buffer sizes (count elements).
//   count: Number of keys/buffers.
//   chunk_size: Size of each value.
//
// Returns: future_id for this batch, or 0 on error.
uint64_t io_engine_submit_batch_set(io_engine_t* engine, const char** keys,
                                    const void** bufs, const size_t* lens,
                                    size_t count, size_t chunk_size);

// Submit a batch EXISTS: check if keys exist.
//
// Args:
//   keys: Array of key strings (count elements).
//   count: Number of keys.
//
// Returns: future_id for this batch, or 0 on error.
//          Completion will contain result_bytes with 0/1 per key.
uint64_t io_engine_submit_batch_exists(io_engine_t* engine, const char** keys,
                                       size_t count);

// ---------------------------------------------------------------------------
// Drain completions
// ---------------------------------------------------------------------------

// Drain all available completions into the provided array.
// Also drains the eventfd.
//
// Args:
//   engine: Engine handle.
//   out: Pre-allocated array of io_completion_t.
//   max_completions: Capacity of the out array.
//
// Returns: Number of completions written to out.
//          Completion error strings and result_bytes are valid until the
//          next call to io_engine_drain_completions.
int io_engine_drain_completions(io_engine_t* engine, io_completion_t* out,
                                size_t max_completions);

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Gracefully close the engine. Idempotent.
// Signals workers to stop, joins threads, closes eventfd.
void io_engine_close(io_engine_t* engine);

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

// Get the last error message (thread-local).
// Returns NULL if no error.
const char* io_engine_last_error(void);

#ifdef __cplusplus
}  // extern "C"
#endif
