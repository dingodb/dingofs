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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_QUEUE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/connection/plug.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class ReceiveQueue final : public Pluggable {
 public:
  explicit ReceiveQueue(QueuePair* queue_pair);

  ReceiveQueue(const ReceiveQueue&) = delete;
  ReceiveQueue& operator=(const ReceiveQueue&) = delete;

  void Start();
  void Shutdown();

  void Submit(ibv_recv_wr* wr);
  void Unplug() override;  // flush all pending work requests

  void Countdown() { --inflights_; }
  uint32_t inflights() const { return inflights_; }

 private:
  static constexpr uint32_t kFlushBatchSize = 32;

  bool running_ = false;
  ibv_qp* qp_ = nullptr;
  uint32_t inflights_ = 0;
  std::vector<ibv_recv_wr*> pending_;
};

using ReceiveQueueUPtr = std::unique_ptr<ReceiveQueue>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_QUEUE_H_
