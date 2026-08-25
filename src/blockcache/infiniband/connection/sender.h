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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SENDER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SENDER_H_

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/connection/send_buffer.h"
#include "blockcache/infiniband/connection/send_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class OpBatch {
 public:
  OpBatch(SendQueue* queue, uint32_t capacity);

  void Add(SendWorkRequest work_request);
  BatchAwaiter Submit();

 private:
  SendQueue* queue_;
  std::vector<SendWorkRequest> work_requests_;
  uint32_t num_wrs_ = 0;
  uint32_t num_read_wrs_ = 0;
};

class MsgSender {
 public:
  MsgSender(QueuePairGroup* qps, SendBufferPool* buffers)
      : send_queue_(qps->GetMsgQueue()), buffers_(buffers) {}

  MsgSender(const MsgSender&) = delete;
  MsgSender& operator=(const MsgSender&) = delete;

  Status Send(SendBuffer* buffer, size_t len);

  void Countdown();

  uint32_t inflights() const { return send_queue_->inflights(); }

 private:
  SendQueue* send_queue_;
  SendBufferPool* buffers_;
};

using MsgSenderUPtr = std::unique_ptr<MsgSender>;

class BulkSender {
 public:
  explicit BulkSender(QueuePairGroup* qps);

  BulkSender(const BulkSender&) = delete;
  BulkSender& operator=(const BulkSender&) = delete;

  Future<Status> Read(std::span<const RemoteRegion> src, BufferView dst) {
    return Move(dst, src, /*is_read=*/true);
  }
  Future<Status> Write(BufferView src, std::span<const RemoteRegion> dst) {
    return Move(src, dst, /*is_read=*/false);
  }

  uint32_t inflights() const { return qps_->bulk_inflights(); }

 private:
  struct Walker {
    ibv_wr_opcode opcode;
    uint32_t lkey;
    std::span<const RemoteRegion> regions;
    char* local_addr;
    uint32_t remaining;
    size_t region_index = 0;

    SendWorkRequest NextWorkRequest(uint32_t max_inline_data);

    bool done() const {
      return remaining == 0 || region_index == regions.size();
    }
    size_t remaining_regions() const { return regions.size() - region_index; }
  };

  Future<Status> Move(BufferView buffer, std::span<const RemoteRegion> regions,
                      bool is_read);
  static OpBatch BuildBatch(SendQueue* queue, uint32_t max_wrs, Walker* walker);
  Status Check(BufferView buffer, std::span<const RemoteRegion> regions) const;

  QueuePairGroup* qps_;
  const MemoryRegistry* registry_;
};

using BulkSenderUPtr = std::unique_ptr<BulkSender>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SENDER_H_
