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

#include <functional>
#include <memory>
#include <vector>

#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"

struct ibv_recv_wr;
struct ibv_send_wr;
struct ibv_sge;

namespace dingofs {
namespace cache {
namespace infiniband {

DECLARE_int32(rdma_send_buffer_size);
DECLARE_int32(rdma_recv_buffer_size);

class Connection {
 public:
  using CompletionHandler = std::function<void(WorkCompletions)>;

  Connection(QueuePairUPtr queue_pair, CompletionQueueUPtr completion_queue);

  Status PostSendWorkRequest(const SendWorkRequest& entry);
  Status PostSendWorkRequests(const std::vector<SendWorkRequest>& entries);
  Status PostRecvWorkRequest(const RecvWorkRequest& entry);
  Status PostRecvWorkRequests(const std::vector<RecvWorkRequest>& entries);
  void HandleCompletion(CompletionHandler handler);

  int GetFd() const { return completion_queue_->GetFd(); }
  QueuePair* GetQueuePair() const { return queue_pair_.get(); }
  RDMABufferPool* GetSendBufferPool() { return send_buffer_pool_.get(); }
  RDMABufferPool* GetRecvBufferPool() { return recv_buffer_pool_.get(); }

 private:
  static Status ValidateSendWorkRequest(const SendWorkRequest& entry);
  static void PrepSendWorkRequest(const SendWorkRequest& entry, ibv_send_wr* wr,
                                  ibv_sge* sge);
  static void PrepRecvWorkRequest(const RecvWorkRequest& entry, ibv_recv_wr* wr,
                                  ibv_sge* sge);
  bool PollCompletionQueue(CompletionHandler handler);

  std::unique_ptr<RDMABufferPool> send_buffer_pool_;
  std::unique_ptr<RDMABufferPool> recv_buffer_pool_;
  std::unique_ptr<CompletionQueue> completion_queue_;
  std::unique_ptr<QueuePair> queue_pair_;
};

using ConnectionUPtr = std::unique_ptr<Connection>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CONNECTION_H_
