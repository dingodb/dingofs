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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_BASE_COMPLETION_QUEUE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_BASE_COMPLETION_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/completion_channel.h"
#include "blockcache/infiniband/base/device.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class CompletionQueue {
 public:
  static StatusOr<CompletionQueue> Create(Device& device,
                                          CompletionChannel& channel);

  CompletionQueue() = default;
  ~CompletionQueue() { Reset(); }

  CompletionQueue(const CompletionQueue&) = delete;
  CompletionQueue& operator=(const CompletionQueue&) = delete;

  CompletionQueue(CompletionQueue&& o) noexcept
      : cq_(o.cq_),
        num_entries_(o.num_entries_),
        num_reserved_(o.num_reserved_),
        num_unacked_(o.num_unacked_) {
    o.cq_ = nullptr;
    o.num_entries_ = 0;
    o.num_reserved_ = 0;
    o.num_unacked_ = 0;
  }
  CompletionQueue& operator=(CompletionQueue&& o) noexcept {
    if (this != &o) {
      Reset();
      cq_ = o.cq_;
      num_entries_ = o.num_entries_;
      num_reserved_ = o.num_reserved_;
      num_unacked_ = o.num_unacked_;
      o.cq_ = nullptr;
      o.num_entries_ = 0;
      o.num_reserved_ = 0;
      o.num_unacked_ = 0;
    }
    return *this;
  }

  int Poll(int n, ibv_wc* wc) { return ibv_poll_cq(cq_, n, wc); }
  int ReqNotify() { return ibv_req_notify_cq(cq_, 0); }
  void DrainEvents();

  Status Reserve(uint32_t wrs, uint32_t device_max_cqe);
  void Unreserve(uint32_t wrs);

  ibv_cq* get() const { return cq_; }

 private:
  CompletionQueue(ibv_cq* cq, uint32_t entries)
      : cq_(cq), num_entries_(entries) {}

  void AckEvents(unsigned n);
  void Reset() noexcept;

  Status GrowSize(uint32_t entries);

  ibv_cq* cq_ = nullptr;
  uint32_t num_entries_ = 0;
  uint32_t num_reserved_ = 0;
  unsigned num_unacked_ = 0;
};

using CompletionQueueUPtr = std::unique_ptr<CompletionQueue>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_BASE_COMPLETION_QUEUE_H_
