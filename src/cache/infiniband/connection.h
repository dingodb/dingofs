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

/*
 * Project: DingoFS
 * Created Date: 2026-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CONNECTION_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CONNECTION_H_

#include <gflags/gflags_declare.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DECLARE_int32(rdma_send_buffer_size);
DECLARE_int32(rdma_recv_buffer_size);

struct WorkCompletion;
using OnCompletion = std::function<void(const WorkCompletion& wc)>;

struct EndPoint {
  std::string device_name;  // IB device, e.g. "mlx5_0"
  uint8_t port_num;         // HCA port, 1-based
};

enum class OpCode : uint8_t {
  kUnknown = 0,
  kSend = 1,
  kRecv = 2,
  kRDMAWrite = 3,
  kRDMARead = 4,
};

struct WorkRequstContext {
  OnCompletion on_completion;
};

struct SendWorkRequest {
  OpCode opcode{OpCode::kUnknown};

  // local
  uint64_t addr{0};
  uint32_t length{0};
  uint32_t lkey{0};

  // remote
  uint64_t raddr{0};
  uint32_t rkey{0};

  bool signaled{false};
  WorkRequstContext* ctx{nullptr};
};

struct RecvWorkRequest {
  // local
  uint64_t addr{0};
  uint32_t length{0};
  uint32_t lkey{0};

  WorkRequstContext* ctx{nullptr};
};

struct WorkCompletion {
  WorkRequstContext* ctx{nullptr};
  OpCode opcode{OpCode::kUnknown};
  Status status{Status::Unknown("unknown")};
  uint32_t byte_len{0};
};

using WorkCompletions = std::vector<WorkCompletion>;

class Connection {
 public:
  using CompletionHandler = std::function<void(WorkCompletions)>;

  Connection(QueuePairUPtr queue_pair, CompletionQueueUPtr completion_queue);

  Status PostSendWorkRequest(const SendWorkRequest& entry) {
    return PostSendWorkRequests(std::vector<SendWorkRequest>{entry});
  }

  Status PostRecvWorkRequest(const RecvWorkRequest& entry) {
    return PostRecvWorkRequests(std::vector<RecvWorkRequest>{entry});
  }

  Status PostSendWorkRequests(const std::vector<SendWorkRequest>& entries);
  Status PostRecvWorkRequests(const std::vector<RecvWorkRequest>& entries);
  void HandleCompletion(CompletionHandler handler);

  int GetFd() const { return completion_queue_->GetFd(); }
  QueuePair* GetQueuePair() const { return queue_pair_.get(); }
  RdmaBufferPool* GetSendBufferPool() { return send_buffer_pool_.get(); }
  RdmaBufferPool* GetRecvBufferPool() { return recv_buffer_pool_.get(); }

 private:
  bool PollCompletionQueue(CompletionHandler handler);

  std::unique_ptr<RdmaBufferPool> send_buffer_pool_;
  std::unique_ptr<RdmaBufferPool> recv_buffer_pool_;
  std::unique_ptr<QueuePair> queue_pair_;
  std::unique_ptr<CompletionQueue> completion_queue_;
};

using ConnectionUPtr = std::unique_ptr<Connection>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CONNECTION_H_
