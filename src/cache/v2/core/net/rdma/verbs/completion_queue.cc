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

#include "cache/v2/core/net/rdma/verbs/completion_queue.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "cache/v2/common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

// Acking takes a per-channel lock, so events are batched.
static constexpr unsigned kAckBatch = 256;

StatusOr<CompletionQueue> CompletionQueue::Create(Device& device,
                                                  uint32_t entries,
                                                  CompletionChannel* channel) {
  entries = std::min(entries, device.max_cqe());
  ibv_cq* cq =
      ibv_create_cq(device.context(), static_cast<int>(entries), nullptr,
                    channel != nullptr ? channel->get() : nullptr, 0);
  if (cq == nullptr) {
    return ToStatus(errno, "create completion queue");
  }
  return CompletionQueue(cq, entries);
}

void CompletionQueue::Reset() noexcept {
  if (cq_ != nullptr) {
    // ibv_destroy_cq blocks until every handed-out event is acked.
    if (unacked_ > 0) {
      ibv_ack_cq_events(cq_, unacked_);
      unacked_ = 0;
    }
    // Not PLOG: ibv_destroy_cq returns the errno value, it does not set errno.
    if (const int rc = ibv_destroy_cq(cq_); rc != 0) {
      LOG(ERROR) << "Fail to destroy completion queue: " << std::strerror(rc);
    }
    cq_ = nullptr;
  }
}

unsigned CompletionQueue::DrainEvents() {
  unsigned taken = 0;
  for (;;) {
    ibv_cq* cq = nullptr;
    void* context = nullptr;
    if (ibv_get_cq_event(cq_->channel, &cq, &context) != 0) {
      // EAGAIN simply means the channel is empty (the fd is non-blocking).
      if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        PLOG(ERROR) << "Fail to get completion queue event";
      }
      break;
    }
    ++taken;
    ++unacked_;
  }
  if (unacked_ >= kAckBatch) {
    AckEvents(unacked_);
  }
  return taken;
}

void CompletionQueue::AckEvents(unsigned n) {
  if (n == 0) {
    return;
  }
  ibv_ack_cq_events(cq_, n);
  unacked_ -= n < unacked_ ? n : unacked_;
}

Status CompletionQueue::EnsureCapacity(uint32_t entries) {
  if (entries <= entries_) {
    return Status::OK();
  }
  if (ibv_resize_cq(cq_, static_cast<int>(entries)) != 0) {
    return ToStatus(errno, "resize completion queue");
  }
  entries_ = entries;
  return Status::OK();
}

Status CompletionQueue::Reserve(uint32_t wrs, uint32_t device_max_cqe) {
  reserved_ += wrs;
  if (reserved_ <= entries_) {
    return Status::OK();
  }
  return EnsureCapacity(std::min(reserved_ * 2, device_max_cqe));
}

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs
