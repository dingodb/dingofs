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

#include "blockcache/infiniband/connection/receiver.h"

#include <glog/logging.h>

#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/common/wr_id.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Receiver::Receiver(QueuePairGroup* qps, ReceiveBufferPool* buffers)
    : buffers_(buffers),
      wrs_(Protocol::MsgRecvWr()),
      sges_(Protocol::MsgRecvWr()),
      queue_(std::make_unique<ReceiveQueue>(qps->GetMsgQueuePair())) {}

void Receiver::Start() {
  const uint32_t n = buffers_->buffer_count();
  CHECK_EQ(n, wrs_.size()) << "the receive ring and its buffer pool disagree";

  LOG(INFO) << "Receiver is starting...";

  for (uint32_t i = 0; i < n; ++i) {
    BuildWorkRequest(static_cast<uint16_t>(i), &wrs_[i]);
  }

  queue_->Start();

  LOG(INFO) << "Successfully start Receiver{buffers=" << n << "}";
}

void Receiver::Shutdown() {
  LOG(INFO) << "Receiver is shutting down...";

  queue_->Shutdown();

  LOG(INFO) << "Successfully shutdown Receiver{inflights="
            << queue_->inflights() << "}";
}

void Receiver::PostAllWorkRequests() {
  for (auto& wr : wrs_) {
    queue_->Submit(&wr);
  }
  queue_->Unplug();
}

void Receiver::BuildWorkRequest(uint16_t index, ibv_recv_wr* work_request) {
  ReceiveBuffer* buffer = &buffers_->Get(index);

  ibv_sge* sge = &sges_[index];
  sge->addr = reinterpret_cast<uint64_t>(buffer->data);
  sge->length = buffer->size;
  sge->lkey = buffers_->lkey();

  work_request->wr_id = MakeWrId(buffer, kTagReceiveBuffer);
  work_request->sg_list = sge;
  work_request->num_sge = 1;
  work_request->next = nullptr;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
