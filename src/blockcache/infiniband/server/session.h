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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_H_

#include <cstdint>
#include <functional>
#include <memory>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/service.h"
#include "blockcache/net/types.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class ReplyWriter final : public ResponseWriter {
 public:
  ReplyWriter(Connection* conn, const MessageView* request,
              Controller* controller)
      : conn_(conn), request_(request), controller_(controller) {}

  Future<bool> Write(const google::protobuf::Message& response) override;

  bool response_sent() const { return response_sent_; }

  ReplyCode ResolveReplyCode(ReplyCode fallback_code) const {
    return failure_code_ != kReplyOk ? failure_code_ : fallback_code;
  }

 private:
  bool response_sent_ = false;
  Connection* conn_;
  const MessageView* request_;
  Controller* controller_;
  ReplyCode failure_code_ = kReplyOk;
};

class Session final {
 public:
  Session(ConnectionUPtr conn, ServiceRegistry* service_registry,
          std::function<Gate::Holder()> enter_gate);

  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;

  Future<Status> Start();
  Future<bool> Shutdown();

  void FailIfIdle(uint64_t idle_ns) { conn_->FailIfIdle(idle_ns); }
  bool Alive() const { return conn_->Alive(); }

 private:
  Status OnMessageReceived(ReceiveBuffer* buffer, const MessageView& message);
  Future<> ProcessRequest(ReceiveBuffer* buffer, MessageView message);
  Future<Status> DispatchRequest(const MessageView& message);
  Future<Status> ReadAttachment(const MessageView& message,
                                Controller* controller);
  Future<ReplyCode> CallMethod(const Service::Method& method,
                               Controller* controller, std::string_view payload,
                               ResponseWriter* writer);
  Future<Status> SendErrorResponse(const MessageView& request,
                                   ReplyCode reply_code);

  bool running_ = false;
  ConnectionUPtr conn_;
  ServiceRegistry* service_registry_;
  std::function<Gate::Holder()> enter_gate_;
};

using SessionUPtr = std::unique_ptr<Session>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
