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
 * Created Date: 2026-04-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_SERVER_SESSION_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_SERVER_SESSION_H_

#include <bthread/execution_queue.h>
#include <google/protobuf/service.h>

#include <memory>

#include "cache/common/slab_buffer.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/protocol.h"
#include "cache/infiniband/reader.h"
#include "cache/infiniband/sender.h"
#include "cache/infiniband/service.h"
#include "cache/iutil/bthread.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class ServerSession : public EventHandler {
 public:
  ServerSession(ConnectionUPtr conn, ServiceHub* service_hub);
  ~ServerSession() override = default;
  Status Start();
  void Shutdown();

  void HandleEvent() override;

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<WorkCompletions>& iter);

  Status OnEstablished();
  void PrepRecvWorkRequest(RDMABuffer* recv_buffer, RecvWorkRequest* wr);
  void OnNewMessage(const WorkCompletion& wc, RDMABuffer* recv_buffer);
  void HandleNewMessage(RDMABuffer* buffer);
  void ParseRequest(Controller* cntl, RDMABuffer* buffer,
                    RequestParser::Result* result);
  void ReadAttachment(Controller* cntl, const std::vector<Region>& src,
                      size_t size);
  void ProcessRequest(google::protobuf::Service* service,
                      const google::protobuf::MethodDescriptor* method,
                      google::protobuf::RpcController* controller,
                      const google::protobuf::Message* request,
                      google::protobuf::Message* response);
  void SendResponse(Controller* cntl, uint64_t correlation_id,
                    google::protobuf::Message* response = nullptr,
                    const Attachment& attachment = {});

  RDMABuffer* AllocReadBuffer(size_t size) {
    return GetGlobalReadSlabPool()->Alloc(size);
  }

  void FreeReadBuffer(RDMABuffer* buffer) {
    GetGlobalReadSlabPool()->Free(buffer);
  }

  ConnectionUPtr conn_;
  std::vector<WorkRequestContext> recv_contexts_;
  RequestParserUPtr request_parser_;
  BodyReaderUPtr body_reader_;
  ResponseSerializerUPtr response_serializer_;
  ResponseSenderUPtr response_sender_;
  iutil::BthreadJoinerUPtr joiner_;
  bthread::ExecutionQueueId<WorkCompletions> queue_id_;
};

using ServerSessionSPtr = std::shared_ptr<ServerSession>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SERVER_SESSION_H_
