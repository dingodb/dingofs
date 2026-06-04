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
#include <mutex>
#include <string_view>
#include <unordered_map>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/memory.h"
#include "cache/iutil/bthread.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class ClientSession : public EventHandler {
 public:
  explicit ClientSession(ConnectionUPtr conn);
  void Start();
  void Shutdown();

  Status OnEstablished();
  void HandleEvent() override;

  void DoCall(Controller* cntl, std::string_view service_name,
              std::string_view method_name,
              const google::protobuf::Message& request,
              google::protobuf::Message* response);

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<WorkCompletions>& iter);

  void PrepRecvWorkRequest(RdmaBuffer* recv_buffer, RecvWorkRequest* wr);

  void SendRequest(Controller* cntl, std::string_view service_name,
                   std::string_view method_name,
                   const google::protobuf::Message& request,
                   RdmaBuffer* send_buffer);
  void OnResponseReceived(const WorkCompletion& wc, RdmaBuffer* buffer);
  void ParseResponse(Controller* cntl, RdmaBuffer* buffer);
  void OnError(Controller* cntl, pb::infiniband::ErrorCode error_code,
               const std::string& reason);

  ConnectionUPtr conn_;
  WorkRequstContext unsignaled_send_wr_context_;
  std::vector<WorkRequstContext> recv_wr_context_;
  bthread::ExecutionQueueId<WorkCompletions> handle_wc_queue_id_;

  // Outstanding requests keyed by correlation id. A response only touches its
  // Controller while the entry is present; on timeout DoCall removes the entry
  // (under the lock) so a late response is dropped instead of dereferencing the
  // already-freed (stack) Controller. A monotonic id avoids pointer reuse (ABA)
  // matching a late response to an unrelated new request.
  std::atomic<uint64_t> next_correlation_id_{1};
  std::atomic<uint64_t> request_send_seq_{0};
  std::mutex outstanding_mutex_;
  std::unordered_map<uint64_t, Controller*> outstanding_;
};

using ClientSessionUPtr = std::unique_ptr<ClientSession>;
using ClientSessionSPtr = std::shared_ptr<ClientSession>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_
