// SPDX-License-Identifier: Apache-2.0

#include "io_engine.h"
#include "file_io.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <stdexcept>

namespace dingofs {

IOEngine::IOEngine(const std::string& base_path, int num_workers,
                   bool use_odirect, SyncMode sync_mode)
    : base_path_(base_path),
      use_odirect_(use_odirect),
      sync_mode_(sync_mode),
      num_workers_(num_workers) {
  if (num_workers_ <= 0) {
    throw std::runtime_error("num_workers must be > 0");
  }

  // Ensure the base directory exists
  io::ensure_directory(base_path_);

  // Create I/O callbacks that capture base_path_, use_odirect_, sync_mode_
  get_cb_ = [this](const std::string& key, void* buf, size_t len,
                   size_t /*chunk_size*/) {
    std::string path = io::key_to_path(base_path_, key);
    io::file_read(path, buf, len, use_odirect_);
  };

  set_cb_ = [this](const std::string& key, const void* buf, size_t len,
                   size_t /*chunk_size*/) {
    io::file_write(base_path_, key, buf, len, use_odirect_, sync_mode_);
  };

  exists_cb_ = [this](const std::string& key) -> bool {
    std::string path = io::key_to_path(base_path_, key);
    return io::file_exists(path);
  };

  efd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (efd_ < 0) {
    throw std::runtime_error("failed to create eventfd");
  }

  start_workers();
}

IOEngine::~IOEngine() { close(); }

void IOEngine::start_workers() {
  workers_.reserve(static_cast<size_t>(num_workers_));
  for (int i = 0; i < num_workers_; ++i) {
    workers_.emplace_back([this]() { this->worker_loop(); });
  }
}

void IOEngine::close() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  stop_.store(true, std::memory_order_release);
  req_cv_.notify_all();

  for (auto& w : workers_) {
    if (w.joinable()) {
      w.join();
    }
  }

  if (efd_ >= 0) {
    ::close(efd_);
    efd_ = -1;
  }

  {
    std::lock_guard<std::mutex> lk(req_mu_);
    while (!requests_.empty()) requests_.pop();
  }
  {
    std::lock_guard<std::mutex> lk(comp_mu_);
    while (!completions_.empty()) completions_.pop();
  }
}

uint64_t IOEngine::submit_batch_rw(Op op,
                                   const std::vector<std::string>& keys,
                                   const std::vector<void*>& bufs,
                                   const std::vector<size_t>& lens,
                                   size_t chunk_size) {
  if (keys.size() != bufs.size() || keys.size() != lens.size()) {
    throw std::runtime_error("keys, bufs, and lens size mismatch");
  }
  if (keys.empty()) {
    throw std::runtime_error("keys list is empty");
  }

  size_t n = keys.size();
  auto [fid, batch, num_tiles, tile_size] = prepare_batch(n, op);

  for (size_t t = 0; t < num_tiles; ++t) {
    size_t start = t * tile_size;
    size_t end = std::min(start + tile_size, n);

    Request req;
    req.op = op;
    req.future_id = fid;
    req.batch = batch;
    req.chunk_size = chunk_size;
    for (size_t i = start; i < end; ++i) {
      req.keys.push_back(keys[i]);
      req.buf_ptrs.push_back(bufs[i]);
      req.buf_lens.push_back(lens[i]);
    }
    enqueue_request(std::move(req));
  }

  return fid;
}

uint64_t IOEngine::submit_batch_get(const std::vector<std::string>& keys,
                                    const std::vector<void*>& bufs,
                                    const std::vector<size_t>& lens,
                                    size_t chunk_size) {
  return submit_batch_rw(Op::GET, keys, bufs, lens, chunk_size);
}

uint64_t IOEngine::submit_batch_set(const std::vector<std::string>& keys,
                                    const std::vector<void*>& bufs,
                                    const std::vector<size_t>& lens,
                                    size_t chunk_size) {
  return submit_batch_rw(Op::SET, keys, bufs, lens, chunk_size);
}

uint64_t IOEngine::submit_batch_exists(
    const std::vector<std::string>& keys) {
  if (keys.empty()) {
    throw std::runtime_error("keys list is empty");
  }

  size_t n = keys.size();
  auto [fid, batch, num_tiles, tile_size] = prepare_batch(n, Op::EXISTS);

  batch->exists_results.assign(n, 0);

  for (size_t t = 0; t < num_tiles; ++t) {
    size_t start = t * tile_size;
    size_t end = std::min(start + tile_size, n);

    Request req;
    req.op = Op::EXISTS;
    req.future_id = fid;
    req.batch = batch;
    req.start_idx = start;
    for (size_t i = start; i < end; ++i) {
      req.keys.push_back(keys[i]);
    }
    enqueue_request(std::move(req));
  }

  return fid;
}

std::vector<Completion> IOEngine::drain_completions() {
  drain_eventfd();

  std::vector<Completion> result;
  for (;;) {
    Completion c;
    {
      std::lock_guard<std::mutex> lk(comp_mu_);
      if (completions_.empty()) {
        signaled_.store(false, std::memory_order_release);
        // Double-check after clearing signal
        if (!completions_.empty() &&
            !signaled_.exchange(true, std::memory_order_acq_rel)) {
          uint64_t x = 1;
          ::write(efd_, &x, sizeof(x));
        }
        break;
      }
      c = std::move(completions_.front());
      completions_.pop();
    }
    result.push_back(std::move(c));
  }

  return result;
}

// --- Private methods ---

std::tuple<uint64_t, std::shared_ptr<BatchState>, size_t, size_t>
IOEngine::prepare_batch(size_t num_items, Op op) {
  size_t num_tiles =
      std::min<size_t>(static_cast<size_t>(num_workers_), num_items);
  size_t tile_size = (num_items + num_tiles - 1) / num_tiles;

  uint64_t fid = next_future_id_.fetch_add(1, std::memory_order_relaxed);
  auto batch = std::make_shared<BatchState>();
  batch->remaining_tiles.store(static_cast<uint32_t>(num_tiles),
                               std::memory_order_relaxed);
  batch->batch_op = op;

  return {fid, batch, num_tiles, tile_size};
}

void IOEngine::enqueue_request(Request&& req) {
  {
    std::lock_guard<std::mutex> lk(req_mu_);
    requests_.push(std::move(req));
  }
  req_cv_.notify_one();
}

void IOEngine::push_completion(Completion&& c) {
  {
    std::lock_guard<std::mutex> lk(comp_mu_);
    completions_.push(std::move(c));
  }
  signal_eventfd();
}

void IOEngine::drain_eventfd() {
  for (;;) {
    uint64_t x;
    ssize_t r = ::read(efd_, &x, sizeof(x));
    if (r == static_cast<ssize_t>(sizeof(x))) continue;
    if (r < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN) break;
    }
    break;
  }
}

void IOEngine::signal_eventfd() {
  if (signaled_.exchange(true, std::memory_order_acq_rel)) return;

  uint64_t x = 1;
  for (;;) {
    ssize_t w = ::write(efd_, &x, sizeof(x));
    if (w == static_cast<ssize_t>(sizeof(x))) return;
    if (w < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error("eventfd write failed");
    }
    throw std::runtime_error("partial write to eventfd");
  }
}

void IOEngine::worker_loop() {
  try {
    for (;;) {
      Request req;
      {
        std::unique_lock<std::mutex> lk(req_mu_);
        req_cv_.wait(lk, [&] {
          return stop_.load(std::memory_order_acquire) || !requests_.empty();
        });
        if (stop_.load(std::memory_order_acquire) && requests_.empty()) {
          break;
        }
        req = std::move(requests_.front());
        requests_.pop();
      }

      Completion comp;
      comp.future_id = req.future_id;

      try {
        switch (req.op) {
          case Op::GET:
            for (size_t i = 0; i < req.keys.size(); ++i) {
              get_cb_(req.keys[i], req.buf_ptrs[i], req.buf_lens[i],
                      req.chunk_size);
            }
            comp.ok = true;
            break;

          case Op::SET:
            for (size_t i = 0; i < req.keys.size(); ++i) {
              set_cb_(req.keys[i], req.buf_ptrs[i], req.buf_lens[i],
                      req.chunk_size);
            }
            comp.ok = true;
            break;

          case Op::EXISTS:
            for (size_t i = 0; i < req.keys.size(); ++i) {
              bool exists = exists_cb_(req.keys[i]);
              req.batch->exists_results[req.start_idx + i] = exists ? 1 : 0;
            }
            comp.ok = true;
            break;
        }
      } catch (const std::exception& e) {
        comp.ok = false;
        comp.error = e.what();
        if (stop_.load(std::memory_order_acquire)) {
          break;
        }
      }

      handle_tile_completion(req, comp);
    }
  } catch (const std::exception& e) {
    fprintf(stderr, "[DingoFS IOEngine Worker Error] %s\n", e.what());
  } catch (...) {
    fprintf(stderr, "[DingoFS IOEngine Worker Error] Unknown exception\n");
  }
}

void IOEngine::handle_tile_completion(const Request& req,
                                      const Completion& comp) {
  if (!comp.ok) {
    req.batch->any_failed.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lk(req.batch->err_mu);
    if (req.batch->first_error.empty()) {
      req.batch->first_error = comp.error;
    }
  }

  uint32_t left =
      req.batch->remaining_tiles.fetch_sub(1, std::memory_order_acq_rel) - 1;

  if (left == 0) {
    Completion batch_comp;
    batch_comp.future_id = req.future_id;
    batch_comp.ok = !req.batch->any_failed.load(std::memory_order_acquire);
    if (!batch_comp.ok) {
      std::lock_guard<std::mutex> lk(req.batch->err_mu);
      batch_comp.error = req.batch->first_error;
    }
    if (req.batch->batch_op == Op::EXISTS) {
      batch_comp.result_bytes = std::move(req.batch->exists_results);
    }
    push_completion(std::move(batch_comp));
  }
}

}  // namespace dingofs
