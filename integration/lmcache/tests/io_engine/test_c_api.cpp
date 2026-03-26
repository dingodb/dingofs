// SPDX-License-Identifier: Apache-2.0
//
// C/C++ tests for the DingoFS connector C API.
// Uses simple assert-based testing (no external framework dependency).

#include "io_engine_capi.h"

#include <sys/eventfd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

// Helper: create a unique temp directory
static std::string make_temp_dir() {
  char tmpl[] = "/tmp/dingofs_test_XXXXXX";
  char* dir = mkdtemp(tmpl);
  assert(dir != nullptr);
  return std::string(dir);
}

// Helper: recursively remove a directory
static void remove_dir(const std::string& path) {
  std::string cmd = "rm -rf " + path;
  (void)system(cmd.c_str());
}

// Helper: drain completions with retry (wait for eventfd)
static std::vector<io_completion_t> drain_with_wait(
    io_engine_t* conn, int timeout_ms = 5000) {
  int efd = io_engine_event_fd(conn);
  assert(efd >= 0);

  std::vector<io_completion_t> all_completions;
  io_completion_t buf[64];

  // Poll eventfd with timeout
  fd_set fds;
  struct timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;

  while (true) {
    FD_ZERO(&fds);
    FD_SET(efd, &fds);

    int ret = select(efd + 1, &fds, nullptr, nullptr, &tv);
    if (ret <= 0) break;  // timeout or error

    int n = io_engine_drain_completions(conn, buf, 64);
    for (int i = 0; i < n; ++i) {
      all_completions.push_back(buf[i]);
    }

    if (n > 0) break;  // Got completions
  }

  return all_completions;
}

// ==========================================================================
// Test: create and destroy
// ==========================================================================
static void test_create_destroy() {
  printf("  test_create_destroy...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 2, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  int efd = io_engine_event_fd(conn);
  assert(efd >= 0);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: single SET + GET round-trip
// ==========================================================================
static void test_single_set_get() {
  printf("  test_single_set_get...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 2, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  // Prepare data
  const size_t data_size = 4096;
  std::vector<uint8_t> write_buf(data_size);
  for (size_t i = 0; i < data_size; ++i) {
    write_buf[i] = static_cast<uint8_t>(i & 0xFF);
  }

  // SET
  const char* key = "test_key_1";
  const char* keys[] = {key};
  const void* wbufs[] = {write_buf.data()};
  size_t lens[] = {data_size};

  uint64_t fid =
      io_engine_submit_batch_set(conn, keys, wbufs, lens, 1, data_size);
  assert(fid != 0);

  auto completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].future_id == fid);
  assert(completions[0].ok == 1);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  void* rbufs[] = {read_buf.data()};

  fid = io_engine_submit_batch_get(conn, keys, rbufs, lens, 1,
                                           data_size);
  assert(fid != 0);

  completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Verify data
  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: EXISTS
// ==========================================================================
static void test_exists() {
  printf("  test_exists...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 2, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  // Write a key
  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 42);

  const char* key = "exists_key";
  const char* keys[] = {key};
  const void* wbufs[] = {buf.data()};
  size_t lens[] = {data_size};

  uint64_t fid =
      io_engine_submit_batch_set(conn, keys, wbufs, lens, 1, data_size);
  auto completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Check EXISTS for existing key
  fid = io_engine_submit_batch_exists(conn, keys, 1);
  completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_bytes != nullptr);
  assert(completions[0].result_len == 1);
  assert(completions[0].result_bytes[0] == 1);

  // Check EXISTS for non-existing key
  const char* missing_key = "no_such_key";
  const char* missing_keys[] = {missing_key};

  fid = io_engine_submit_batch_exists(conn, missing_keys, 1);
  completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_bytes[0] == 0);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: batch operations
// ==========================================================================
static void test_batch_operations() {
  printf("  test_batch_operations...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 4, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  const size_t num_keys = 10;
  const size_t data_size = 2048;

  // Prepare keys and data
  std::vector<std::string> key_strs(num_keys);
  std::vector<const char*> key_ptrs(num_keys);
  std::vector<std::vector<uint8_t>> write_bufs(num_keys);
  std::vector<const void*> wbuf_ptrs(num_keys);
  std::vector<size_t> lens(num_keys, data_size);

  for (size_t i = 0; i < num_keys; ++i) {
    key_strs[i] = "batch_key_" + std::to_string(i);
    key_ptrs[i] = key_strs[i].c_str();
    write_bufs[i].resize(data_size);
    for (size_t j = 0; j < data_size; ++j) {
      write_bufs[i][j] = static_cast<uint8_t>((i * 7 + j) & 0xFF);
    }
    wbuf_ptrs[i] = write_bufs[i].data();
  }

  // Batch SET
  uint64_t fid = io_engine_submit_batch_set(
      conn, key_ptrs.data(), wbuf_ptrs.data(), lens.data(), num_keys,
      data_size);
  assert(fid != 0);

  auto completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Batch EXISTS
  fid = io_engine_submit_batch_exists(conn, key_ptrs.data(), num_keys);
  completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_len == num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    assert(completions[0].result_bytes[i] == 1);
  }

  // Batch GET
  std::vector<std::vector<uint8_t>> read_bufs(num_keys);
  std::vector<void*> rbuf_ptrs(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    read_bufs[i].resize(data_size, 0);
    rbuf_ptrs[i] = read_bufs[i].data();
  }

  fid = io_engine_submit_batch_get(
      conn, key_ptrs.data(), rbuf_ptrs.data(), lens.data(), num_keys,
      data_size);
  completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Verify all data
  for (size_t i = 0; i < num_keys; ++i) {
    assert(memcmp(write_bufs[i].data(), read_bufs[i].data(), data_size) == 0);
  }

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: concurrent operations (multiple batches in-flight)
// ==========================================================================
static void test_concurrent_batches() {
  printf("  test_concurrent_batches...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 8, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  const size_t data_size = 4096;
  const int num_batches = 5;

  // Submit multiple SET batches without draining
  std::vector<uint64_t> fids;
  std::vector<std::vector<uint8_t>> all_bufs;
  std::vector<std::string> all_keys;

  for (int b = 0; b < num_batches; ++b) {
    std::string key = "concurrent_key_" + std::to_string(b);
    all_keys.push_back(key);

    std::vector<uint8_t> buf(data_size);
    for (size_t i = 0; i < data_size; ++i) {
      buf[i] = static_cast<uint8_t>((b * 13 + i) & 0xFF);
    }
    all_bufs.push_back(std::move(buf));

    const char* keys[] = {all_keys.back().c_str()};
    const void* wbufs[] = {all_bufs.back().data()};
    size_t lens[] = {data_size};

    uint64_t fid = io_engine_submit_batch_set(conn, keys, wbufs, lens,
                                                       1, data_size);
    assert(fid != 0);
    fids.push_back(fid);
  }

  // Drain all completions
  int total_completed = 0;
  int retries = 0;
  while (total_completed < num_batches && retries < 50) {
    auto completions = drain_with_wait(conn, 500);
    for (const auto& c : completions) {
      assert(c.ok == 1);
      total_completed++;
    }
    retries++;
  }
  assert(total_completed == num_batches);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: close idempotency
// ==========================================================================
static void test_close_idempotent() {
  printf("  test_close_idempotent...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 2, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  // Close multiple times
  io_engine_close(conn);
  io_engine_close(conn);
  io_engine_close(conn);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: error handling
// ==========================================================================
static void test_error_handling() {
  printf("  test_error_handling...");

  // NULL arguments
  uint64_t fid = io_engine_submit_batch_get(nullptr, nullptr, nullptr,
                                                     nullptr, 0, 0);
  assert(fid == 0);
  const char* err = io_engine_last_error();
  assert(err != nullptr);

  // GET on non-existent key should fail
  std::string dir = make_temp_dir();
  io_engine_t* conn =
      io_engine_create(dir.c_str(), 2, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 0);
  const char* key = "nonexistent_key";
  const char* keys[] = {key};
  void* rbufs[] = {buf.data()};
  size_t lens[] = {data_size};

  fid = io_engine_submit_batch_get(conn, keys, rbufs, lens, 1,
                                            data_size);
  assert(fid != 0);

  auto completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 0);  // Should fail
  assert(completions[0].error != nullptr);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Test: large data (simulating KV cache chunks)
// ==========================================================================
static void test_large_data() {
  printf("  test_large_data...");
  std::string dir = make_temp_dir();

  io_engine_t* conn =
      io_engine_create(dir.c_str(), 4, 0, IO_ENGINE_SYNC_ALWAYS);
  assert(conn != nullptr);

  // 1 MB chunk (typical KV cache size)
  const size_t data_size = 1024 * 1024;
  std::vector<uint8_t> write_buf(data_size);
  for (size_t i = 0; i < data_size; ++i) {
    write_buf[i] = static_cast<uint8_t>((i * 17) & 0xFF);
  }

  const char* key = "large_chunk";
  const char* keys[] = {key};
  const void* wbufs[] = {write_buf.data()};
  size_t lens[] = {data_size};

  // SET
  uint64_t fid =
      io_engine_submit_batch_set(conn, keys, wbufs, lens, 1, data_size);
  auto completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  void* rbufs[] = {read_buf.data()};

  fid = io_engine_submit_batch_get(conn, keys, rbufs, lens, 1,
                                           data_size);
  completions = drain_with_wait(conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);

  io_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ==========================================================================
// Main
// ==========================================================================
int main() {
  printf("Running DingoFS IOEngine C API tests:\n");

  test_create_destroy();
  test_single_set_get();
  test_exists();
  test_batch_operations();
  test_concurrent_batches();
  test_close_idempotent();
  test_error_handling();
  test_large_data();

  printf("\nAll tests passed!\n");
  return 0;
}
