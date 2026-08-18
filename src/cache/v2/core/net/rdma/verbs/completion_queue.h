/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_COMPLETION_QUEUE_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_COMPLETION_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/verbs/completion_channel.h"
#include "cache/v2/core/net/rdma/verbs/device.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

// One CQ per shard: reaping needs no lookup table and no lock.
// Teardown order: every QP feeding this CQ must be destroyed before it.
class CompletionQueue {
 public:
  // channel may be null (poll-only CQ, used by tests).
  static StatusOr<CompletionQueue> Create(Device& device, uint32_t entries,
                                          CompletionChannel* channel);

  CompletionQueue() = default;
  ~CompletionQueue() { Reset(); }

  CompletionQueue(const CompletionQueue&) = delete;
  CompletionQueue& operator=(const CompletionQueue&) = delete;

  CompletionQueue(CompletionQueue&& o) noexcept
      : cq_(o.cq_),
        entries_(o.entries_),
        reserved_(o.reserved_),
        unacked_(o.unacked_) {
    o.cq_ = nullptr;
    o.entries_ = 0;
    o.reserved_ = 0;
    o.unacked_ = 0;
  }
  CompletionQueue& operator=(CompletionQueue&& o) noexcept {
    if (this != &o) {
      Reset();
      cq_ = o.cq_;
      entries_ = o.entries_;
      reserved_ = o.reserved_;
      unacked_ = o.unacked_;
      o.cq_ = nullptr;
      o.entries_ = 0;
      o.reserved_ = 0;
      o.unacked_ = 0;
    }
    return *this;
  }

  int Poll(int n, ibv_wc* wc) { return ibv_poll_cq(cq_, n, wc); }

  // Arms the channel for one notification. Returns 0 or errno.
  int ReqNotify() { return ibv_req_notify_cq(cq_, 0); }

  // Drains pending channel events; returns count. Acks are batched.
  unsigned DrainEvents();
  void AckEvents(unsigned n);

  // CQ overrun is unrecoverable; grows, never shrinks, to >= `entries`.
  Status EnsureCapacity(uint32_t entries);

  // Books `wrs` more outstanding WRs; grows the queue with headroom.
  Status Reserve(uint32_t wrs, uint32_t device_max_cqe);

  // Releases a retired connection's booking; capacity itself is kept.
  void Unreserve(uint32_t wrs) {
    reserved_ -= wrs < reserved_ ? wrs : reserved_;
  }

  bool Valid() const { return cq_ != nullptr; }
  ibv_cq* get() const { return cq_; }

 private:
  CompletionQueue(ibv_cq* cq, uint32_t entries) : cq_(cq), entries_(entries) {}
  void Reset() noexcept;

  ibv_cq* cq_ = nullptr;
  uint32_t entries_ = 0;
  uint32_t reserved_ = 0;
  unsigned unacked_ = 0;
};

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_COMPLETION_QUEUE_H_
