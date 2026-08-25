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

#include "blockcache/infiniband/connection/receive_queue.h"

#include <glog/logging.h>

#include <cstring>

#include "blockcache/infiniband/base/queue_pair.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

ReceiveQueue::ReceiveQueue(QueuePair* queue_pair) : qp_(queue_pair->get()) {
  pending_.reserve(kFlushBatchSize);
}

void ReceiveQueue::Start() {
  LOG(INFO) << "ReceiveQueue{qpn=" << qp_->qp_num << "} is starting...";

  running_ = true;

  LOG(INFO) << "Successfully start ReceiveQueue{qpn=" << qp_->qp_num
            << " batch_size=" << kFlushBatchSize << "}";
}

void ReceiveQueue::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "ReceiveQueue{qpn=" << qp_->qp_num << "} is shutting down...";

  running_ = false;

  LOG(INFO) << "Successfully shutdown ReceiveQueue{qpn=" << qp_->qp_num
            << " inflights=" << inflights_ << "}";
}

void ReceiveQueue::Submit(ibv_recv_wr* wr) {
  if (!running_) {
    return;
  }

  pending_.push_back(wr);
  if (pending_.size() < kFlushBatchSize) {
    Plug();
  } else {
    Unplug();
  }
}

void ReceiveQueue::Unplug() {
  if (pending_.empty() || !running_) {
    pending_.clear();
    return;
  }

  for (size_t i = 0; i + 1 < pending_.size(); ++i) {
    pending_[i]->next = pending_[i + 1];
  }
  pending_.back()->next = nullptr;

  ibv_recv_wr* bad = nullptr;
  int rc = ibv_post_recv(qp_, pending_.front(), &bad);
  if (rc != 0) {
    LOG(ERROR) << "Fail to post recv work request: " << std::strerror(rc);
    pending_.clear();
    Shutdown();
    return;
  }

  inflights_ += static_cast<uint32_t>(pending_.size());
  pending_.clear();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
