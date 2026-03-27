// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <sys/eventfd.h>
#include <unistd.h>

#include "file_io.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace dingofs {

// Operation type for batch requests
enum class Op : uint8_t { GET, SET, EXISTS };

// Shared state for coordinating tiles within a single batch operation
struct BatchState {
  std::atomic<uint32_t> remaining_tiles{0};
  std::atomic<bool> any_failed{false};

  std::mutex err_mu;
  std::string first_error;

  // For EXISTS: per-key results (0 or 1). Not vector<bool> to avoid data race.
  std::vector<uint8_t> exists_results;

  Op batch_op;
};

// A request submitted to the I/O engine
struct Request {
  uint64_t future_id = 0;
  Op op;

  std::vector<std::string> keys;
  std::vector<void*> buf_ptrs;
  std::vector<size_t> buf_lens;

  std::shared_ptr<BatchState> batch;
  size_t start_idx = 0;  // For EXISTS: offset into exists_results
  size_t chunk_size = 0;
};

// A completion returned from the I/O engine
struct Completion {
  uint64_t future_id = 0;
  bool ok = true;
  std::vector<uint8_t> result_bytes;  // For EXISTS
  std::string error;
};

// Multi-threaded I/O engine with batch tiling, eventfd-based async completion
// signaling, and pluggable file I/O callbacks.
//
// Divides batch operations into tiles across worker threads, coordinates tile
// completion via atomic counters, and notifies the caller via eventfd when
// a batch completes.
class IOEngine {
 public:
  IOEngine(const std::string& base_path, int num_workers, bool use_odirect,
           SyncMode sync_mode = SyncMode::ALWAYS);
  ~IOEngine();

  IOEngine(const IOEngine&) = delete;
  IOEngine& operator=(const IOEngine&) = delete;

  // Returns the eventfd for async notification (readable when completions are
  // available).
  int event_fd() const { return efd_; }

  // Submit a batch GET operation. Returns a future_id.
  uint64_t submit_batch_get(const std::vector<std::string>& keys,
                            const std::vector<void*>& bufs,
                            const std::vector<size_t>& lens,
                            size_t chunk_size);

  // Submit a batch SET operation. Returns a future_id.
  uint64_t submit_batch_set(const std::vector<std::string>& keys,
                            const std::vector<void*>& bufs,
                            const std::vector<size_t>& lens,
                            size_t chunk_size);

  // Submit a batch EXISTS operation. Returns a future_id.
  uint64_t submit_batch_exists(const std::vector<std::string>& keys);

  // Drain all available completions. Also drains the eventfd.
  std::vector<Completion> drain_completions();

  // Graceful shutdown. Idempotent.
  void close();

 private:
  // I/O callback signatures
  using GetCallback = std::function<void(const std::string& key, void* buf,
                                         size_t len, size_t chunk_size)>;
  using SetCallback =
      std::function<void(const std::string& key, const void* buf, size_t len,
                         size_t chunk_size)>;
  using ExistsCallback = std::function<bool(const std::string& key)>;

  void start_workers();
  void worker_loop();

  // Tiling: divide work among workers
  std::tuple<uint64_t, std::shared_ptr<BatchState>, size_t, size_t>
  prepare_batch(size_t num_items, Op op);

  uint64_t submit_batch_rw(Op op, const std::vector<std::string>& keys,
                           const std::vector<void*>& bufs,
                           const std::vector<size_t>& lens,
                           size_t chunk_size);

  void enqueue_request(Request&& req);
  void push_completion(Completion&& c);
  void handle_tile_completion(const Request& req, const Completion& comp);

  void drain_eventfd();
  void signal_eventfd();

  std::string base_path_;
  bool use_odirect_;
  SyncMode sync_mode_;

  int num_workers_;
  int efd_ = -1;
  std::atomic<bool> signaled_{false};
  std::atomic<bool> stop_{false};
  std::atomic<bool> closed_{false};
  std::atomic<uint64_t> next_future_id_{1};

  // Submission queue
  std::mutex req_mu_;
  std::condition_variable req_cv_;
  std::queue<Request> requests_;

  // Completion queue
  std::mutex comp_mu_;
  std::queue<Completion> completions_;

  std::vector<std::thread> workers_;

  // I/O callbacks
  GetCallback get_cb_;
  SetCallback set_cb_;
  ExistsCallback exists_cb_;
};

}  // namespace dingofs
