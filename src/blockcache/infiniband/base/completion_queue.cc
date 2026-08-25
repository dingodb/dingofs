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

#include "blockcache/infiniband/base/completion_queue.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(rdma_cq_entries, 4096,
              "initial completion queue entries; grows on demand");
DEFINE_validator(rdma_cq_entries, [](const char* /*name*/, uint32_t value) {
  return value > 0;
});

namespace infiniband {

StatusOr<CompletionQueue> CompletionQueue::Create(Device& device,
                                                  CompletionChannel& channel) {
  const uint32_t entries = std::min(FLAGS_rdma_cq_entries, device.max_cqe());
  ibv_cq* cq = ibv_create_cq(device.context(), static_cast<int>(entries),
                             nullptr, channel.get(), 0);
  if (cq == nullptr) {
    return ToStatus(errno, "create completion queue");
  }

  LOG(INFO) << "Successfully create CompletionQueue{device=" << device.name()
            << " entries=" << entries << "}";
  return CompletionQueue(cq, entries);
}

void CompletionQueue::DrainEvents() {
  static constexpr unsigned kAckBatch = 256;

  for (;;) {
    ibv_cq* cq = nullptr;
    void* context = nullptr;
    if (ibv_get_cq_event(cq_->channel, &cq, &context) != 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        PLOG(ERROR) << "Fail to get completion queue event";
      }
      break;
    }

    ++num_unacked_;
  }

  if (num_unacked_ >= kAckBatch) {
    AckEvents(num_unacked_);
  }
}

Status CompletionQueue::Reserve(uint32_t wrs, uint32_t device_max_cqe) {
  num_reserved_ += wrs;
  if (num_reserved_ <= num_entries_) {
    return Status::OK();
  }
  return GrowSize(std::min(num_reserved_ * 2, device_max_cqe));
}

void CompletionQueue::Unreserve(uint32_t wrs) {
  num_reserved_ -= wrs < num_reserved_ ? wrs : num_reserved_;
}

void CompletionQueue::AckEvents(unsigned n) {
  if (n == 0) {
    return;
  }

  ibv_ack_cq_events(cq_, n);
  num_unacked_ -= n < num_unacked_ ? n : num_unacked_;
}

void CompletionQueue::Reset() noexcept {
  if (cq_ == nullptr) {
    return;
  }

  // ibv_destroy_cq blocks until every handed-out event is acked.
  if (num_unacked_ > 0) {
    ibv_ack_cq_events(cq_, num_unacked_);
    num_unacked_ = 0;
  }

  const int rc = ibv_destroy_cq(cq_);
  if (rc != 0) {
    LOG(ERROR) << "Fail to destroy completion queue: " << std::strerror(rc);
  }
  cq_ = nullptr;
}

Status CompletionQueue::GrowSize(uint32_t entries) {
  if (entries <= num_entries_) {
    return Status::OK();
  }

  if (ibv_resize_cq(cq_, static_cast<int>(entries)) != 0) {
    return ToStatus(errno, "resize completion queue");
  }

  num_entries_ = entries;
  return Status::OK();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
