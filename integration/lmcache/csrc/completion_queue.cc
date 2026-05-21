// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "completion_queue.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <stdexcept>
#include <utility>

namespace dingofs {
namespace integration {
namespace lmcache {

namespace {

void EventFdNotify(int fd) {
  uint64_t one = 1;
  // EFD_NONBLOCK + 8-byte write: never blocks; only fails on bad fd.
  ssize_t n = ::write(fd, &one, sizeof(one));
  (void)n;  // counter saturates around 2^64; loss is impossible in practice
}

}  // namespace

// ---------- FanIn ----------

FanIn::FanIn(uint64_t future_id, OpType op, size_t total,
             CompletionQueue* queue)
    : future_id_(future_id),
      op_(op),
      total_(total),
      remaining_(total),
      per_key_(total, 0),
      queue_(queue) {}

void FanIn::Report(size_t idx, bool ok, std::string err) {
  per_key_[idx] = ok ? 1 : 0;

  if (!ok) {
    bool expected = true;
    if (ok_.compare_exchange_strong(expected, false,
                                    std::memory_order_acq_rel)) {
      // First failure wins the error string.
      std::lock_guard<bthread::Mutex> g(error_mu_);
      error_ = std::move(err);
    }
  }

  // acq_rel so the per_key_ write above is visible when the consumer reads
  // through Drain().
  if (remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    Completion c{future_id_, op_, ok_.load(std::memory_order_acquire),
                 std::move(error_), std::move(per_key_)};
    queue_->Push(std::move(c));
    delete this;
  }
}

// ---------- CompletionQueue ----------

CompletionQueue::CompletionQueue() {
  efd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (efd_ < 0) {
    throw std::runtime_error("eventfd() failed");
  }
}

CompletionQueue::~CompletionQueue() {
  if (efd_ >= 0) {
    ::close(efd_);
  }
}

FanIn* CompletionQueue::NewFanIn(OpType op, size_t n) {
  uint64_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
  return new FanIn(id, op, n, this);
}

uint64_t CompletionQueue::PushEmpty(OpType op) {
  uint64_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
  Push(Completion{id, op, /*ok=*/true, /*error=*/{}, /*per_key=*/{}});
  return id;
}

void CompletionQueue::Push(Completion c) {
  {
    std::lock_guard<bthread::Mutex> g(mu_);
    queue_.push_back(std::move(c));
  }
  EventFdNotify(efd_);
}

std::vector<Completion> CompletionQueue::Drain() {
  // Read efd BEFORE draining the queue. Order matters: if we drained first
  // and then read, a Push that landed between the two would have its efd
  // write swallowed by our read, leaving its completion in queue_ with no
  // future poll wakeup (epoll is level-triggered on counter > 0).
  // Reading first means a racing Push at worst causes one spurious wakeup
  // with an empty queue — harmless.
  uint64_t sink;
  ssize_t n = ::read(efd_, &sink, sizeof(sink));
  (void)n;

  std::vector<Completion> out;
  {
    std::lock_guard<bthread::Mutex> g(mu_);
    out.reserve(queue_.size());
    while (!queue_.empty()) {
      out.push_back(std::move(queue_.front()));
      queue_.pop_front();
    }
  }
  return out;
}

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs
