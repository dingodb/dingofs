// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_INTEGRATION_LMCACHE_COMPLETION_QUEUE_H_
#define DINGOFS_INTEGRATION_LMCACHE_COMPLETION_QUEUE_H_

#include <bthread/mutex.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

namespace dingofs {
namespace integration {
namespace lmcache {

enum class OpType : uint8_t {
  kSet = 0,
  kGet = 1,
  kExists = 2,
};

struct Completion {
  uint64_t future_id;
  OpType op;
  bool ok;                       // batch-level: false if any per-key failed
  std::string error;             // first non-ok message; empty when ok
  std::vector<uint8_t> per_key;  // per-key: 1 = ok, 0 = fail
};

class CompletionQueue;

// FanIn aggregates N per-key async completions into a single Completion
// event delivered via the parent CompletionQueue.
//
// Lifecycle:
//   1) Caller obtains a FanIn* from CompletionQueue::NewFanIn(op, n).
//   2) Caller issues exactly `n` async I/Os, each holding a pointer to this
//      FanIn and its slot index.
//   3) Each I/O completion calls fan->Report(idx, ok, err). The last call
//      pushes the Completion onto the queue, signals the eventfd, and
//      `delete this` (no caller cleanup needed).
//
// std::vector<bool>'s bit packing is not thread-safe under per-element
// concurrent writes, so per_key uses uint8_t.
class FanIn {
 public:
  FanIn(uint64_t future_id, OpType op, size_t total, CompletionQueue* queue);
  ~FanIn() = default;

  FanIn(const FanIn&) = delete;
  FanIn& operator=(const FanIn&) = delete;

  // Report one slot's outcome. Thread-safe.
  void Report(size_t idx, bool ok, std::string err = {});

  uint64_t future_id() const { return future_id_; }

 private:
  uint64_t future_id_;
  OpType op_;
  size_t total_;
  std::atomic<size_t> remaining_;
  std::vector<uint8_t> per_key_;
  std::atomic<bool> ok_{true};
  bthread::Mutex error_mu_;
  std::string error_;
  CompletionQueue* queue_;  // not owning
};

class CompletionQueue {
 public:
  CompletionQueue();
  ~CompletionQueue();

  CompletionQueue(const CompletionQueue&) = delete;
  CompletionQueue& operator=(const CompletionQueue&) = delete;

  int event_fd() const { return efd_; }

  // Allocate a FanIn. Heap-owned; freed by the last FanIn::Report.
  // Precondition: n > 0. Use PushEmpty() for zero-key batches.
  FanIn* NewFanIn(OpType op, size_t n);

  // Push an immediate empty-batch completion (n == 0 fast path) and return
  // its future_id. Avoids allocating a zero-sized FanIn whose Report() would
  // never be invoked.
  uint64_t PushEmpty(OpType op);

  // Drain queued completions; called from Python via drain_completions().
  std::vector<Completion> Drain();

  // Used by FanIn on last Report.
  void Push(Completion c);

 private:
  int efd_;
  bthread::Mutex mu_;
  std::deque<Completion> queue_;
  std::atomic<uint64_t> next_id_{1};
};

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs

#endif  // DINGOFS_INTEGRATION_LMCACHE_COMPLETION_QUEUE_H_
