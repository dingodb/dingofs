// SPDX-License-Identifier: Apache-2.0

#include "io_engine_capi.h"
#include "io_engine.h"

#include <cstring>
#include <exception>
#include <string>
#include <vector>

// Thread-local error buffer for last_error reporting
static thread_local std::string tl_last_error;

static void set_error(const std::string& msg) { tl_last_error = msg; }
static void clear_error() { tl_last_error.clear(); }

// The opaque handle wraps the C++ IOEngine plus a cache for
// completion data that must outlive drain_completions calls.
struct io_engine_t {
  dingofs::IOEngine engine;

  // Cache for drain_completions: we keep the C++ Completions alive so
  // that error strings and result_bytes pointers remain valid until the
  // next drain call.
  std::vector<dingofs::Completion> cached_completions;

  io_engine_t(const std::string& base_path, int num_workers, bool use_odirect,
              dingofs::SyncMode sync_mode)
      : engine(base_path, num_workers, use_odirect, sync_mode) {}
};

extern "C" {

io_engine_t* io_engine_create(const char* base_path, int num_workers,
                              int use_odirect, int sync_mode) {
  clear_error();
  try {
    auto sm = static_cast<dingofs::SyncMode>(sync_mode);
    return new io_engine_t(std::string(base_path), num_workers,
                           use_odirect != 0, sm);
  } catch (const std::exception& e) {
    set_error(e.what());
    return nullptr;
  }
}

void io_engine_destroy(io_engine_t* engine) {
  if (engine) {
    try {
      engine->engine.close();
    } catch (...) {
      // Best effort
    }
    delete engine;
  }
}

int io_engine_event_fd(const io_engine_t* engine) {
  if (!engine) return -1;
  return engine->engine.event_fd();
}

uint64_t io_engine_submit_batch_get(io_engine_t* engine, const char** keys,
                                    void** bufs, const size_t* lens,
                                    size_t count, size_t chunk_size) {
  clear_error();
  if (!engine || !keys || !bufs || !lens || count == 0) {
    set_error("invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> key_vec(keys, keys + count);
    std::vector<void*> buf_vec(bufs, bufs + count);
    std::vector<size_t> len_vec(lens, lens + count);
    return engine->engine.submit_batch_get(key_vec, buf_vec, len_vec,
                                           chunk_size);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

uint64_t io_engine_submit_batch_set(io_engine_t* engine, const char** keys,
                                    const void** bufs, const size_t* lens,
                                    size_t count, size_t chunk_size) {
  clear_error();
  if (!engine || !keys || !bufs || !lens || count == 0) {
    set_error("invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> key_vec(keys, keys + count);
    // Cast const void** to void** for the internal API (SET treats bufs as
    // read-only, the const is stripped at the C boundary for simplicity)
    std::vector<void*> buf_vec(count);
    for (size_t i = 0; i < count; ++i) {
      buf_vec[i] = const_cast<void*>(bufs[i]);
    }
    std::vector<size_t> len_vec(lens, lens + count);
    return engine->engine.submit_batch_set(key_vec, buf_vec, len_vec,
                                           chunk_size);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

uint64_t io_engine_submit_batch_exists(io_engine_t* engine, const char** keys,
                                       size_t count) {
  clear_error();
  if (!engine || !keys || count == 0) {
    set_error("invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> key_vec(keys, keys + count);
    return engine->engine.submit_batch_exists(key_vec);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

int io_engine_drain_completions(io_engine_t* engine, io_completion_t* out,
                                size_t max_completions) {
  clear_error();
  if (!engine || !out || max_completions == 0) {
    return 0;
  }
  try {
    // Get completions from C++ side. Cache them so pointers remain valid.
    engine->cached_completions = engine->engine.drain_completions();

    size_t n = std::min(engine->cached_completions.size(), max_completions);
    for (size_t i = 0; i < n; ++i) {
      const auto& c = engine->cached_completions[i];
      out[i].future_id = c.future_id;
      out[i].ok = c.ok ? 1 : 0;
      out[i].error = c.error.empty() ? nullptr : c.error.c_str();
      out[i].result_bytes =
          c.result_bytes.empty() ? nullptr : c.result_bytes.data();
      out[i].result_len = c.result_bytes.size();
    }
    return static_cast<int>(n);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

void io_engine_close(io_engine_t* engine) {
  if (engine) {
    try {
      engine->engine.close();
    } catch (...) {
      // Best effort
    }
  }
}

const char* io_engine_last_error(void) {
  return tl_last_error.empty() ? nullptr : tl_last_error.c_str();
}

}  // extern "C"
