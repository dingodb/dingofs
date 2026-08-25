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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVER_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/infiniband/connection/receive_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Receiver {
 public:
  Receiver(QueuePairGroup* qps, ReceiveBufferPool* buffers);

  Receiver(const Receiver&) = delete;
  Receiver& operator=(const Receiver&) = delete;

  void Start();
  void Shutdown();

  void PostAllWorkRequests();

  void RepostWorkRequest(ReceiveBuffer* buffer) {
    queue_->Submit(&wrs_[buffer->index]);
  }

  void Countdown() { queue_->Countdown(); }
  uint32_t inflights() const { return queue_->inflights(); }

 private:
  void BuildWorkRequest(uint16_t index, ibv_recv_wr* work_request);

  ReceiveBufferPool* buffers_;
  std::vector<ibv_recv_wr> wrs_;
  std::vector<ibv_sge> sges_;
  ReceiveQueueUPtr queue_;
};

using ReceiverUPtr = std::unique_ptr<Receiver>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVER_H_
