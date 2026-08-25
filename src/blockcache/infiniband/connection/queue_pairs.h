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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_QUEUE_PAIRS_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_QUEUE_PAIRS_H_

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/connection/send_queue.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// 1 msg queue pair + N bulk queue pair
// also one queue pair own one send queue
class QueuePairGroup {
 public:
  static StatusOr<QueuePairGroup> Create(Device* device,
                                         CompletionQueue* completion_queue);

  QueuePairGroup() = default;

  QueuePairGroup(const QueuePairGroup&) = delete;
  QueuePairGroup& operator=(const QueuePairGroup&) = delete;
  QueuePairGroup(QueuePairGroup&&) = default;
  QueuePairGroup& operator=(QueuePairGroup&&) = default;

  Status ModifyToReady(std::span<const QueuePairInfo> peers);
  void ModifyToError();

  SendQueue* GetMsgQueue() { return &msg_.queue; }
  QueuePair* GetMsgQueuePair() { return &msg_.queue_pair; }
  SendQueue* NextBulkQueue();

  uint32_t bulk_inflights() const;
  uint8_t qp_count() const { return static_cast<uint8_t>(1 + bulks_.size()); }
  std::vector<QueuePairInfo> GetInfos() const;

 private:
  struct QpRail {
    QueuePair queue_pair;
    SendQueue queue;
  };

  static StatusOr<QpRail> CreateQpRail(Device* device,
                                       CompletionQueue* completion_queue,
                                       const QueuePairOption& option);
  static Status ModifyToReady(QpRail* rail, const QueuePairInfo& peer);

  QpRail msg_;
  std::vector<QpRail> bulks_;
  unsigned next_bulk_ = 0;
};

using QueuePairGroupUPtr = std::unique_ptr<QueuePairGroup>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_QUEUE_PAIRS_H_
