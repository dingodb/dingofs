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
 * Created Date: 2026-04-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_

#include <bthread/execution_queue.h>
#include <google/protobuf/message.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string_view>

#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/protocol.h"
#include "cache/infiniband/sender.h"
#include "cache/infiniband/waiter.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class ClientSession : public EventHandler {
 public:
  explicit ClientSession(ConnectionUPtr conn);
  Status Start();
  void Shutdown();

  void HandleEvent() override;

  void DoCall(Controller* cntl, std::string_view service_name,
              std::string_view method_name,
              const google::protobuf::Message& request,
              google::protobuf::Message* response);

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<WorkCompletions>& iter);

  Status OnEstablished();
  void SendRequest(Controller* cntl, uint64_t correlation_id,
                   std::string_view service_name, std::string_view method_name,
                   const google::protobuf::Message& request);
  void WaitingResponse(Waiter* Waiter);
  // Idempotently transition the QP to error so the client HCA stops servicing
  // late one-sided RDMA from the server (memory-safety fence on timeout).
  void MarkBroken();
  void PrepRecvWorkRequest(RDMABuffer* recv_buffer, RecvWorkRequest* wr);
  void OnResponseReceived(const WorkCompletion& wc, RDMABuffer* buffer);
  void ParseResponse(Controller* cntl, RDMABuffer* recv_buffer,
                     google::protobuf::Message* response);

  uint64_t GetNextCorrelationId() {
    return next_correlation_id_.fetch_add(1, std::memory_order_relaxed);
  }

  ConnectionUPtr conn_;
  std::atomic<bool> broken_{false};
  std::atomic<uint64_t> next_correlation_id_;
  std::vector<WorkRequestContext> recv_contexts_;
  RequestSerializerUPtr request_serializer_;
  RequestSenderUPtr request_sender_;
  ResponseParserUPtr response_parser_;
  bthread::ExecutionQueueId<WorkCompletions> queue_id_;
  WaitersUPtr waiters_;
};

using ClientSessionUPtr = std::unique_ptr<ClientSession>;
using ClientSessionSPtr = std::shared_ptr<ClientSession>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_
