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

#include "blockcache/infiniband/server/session.h"

#include <glog/logging.h>
#include <google/protobuf/message.h>

#include <cerrno>
#include <memory>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/service.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Future<bool> ReplyWriter::Write(const google::protobuf::Message& response) {
  if (!controller_->response_attachment().empty()) {
    const BufferView attachment = controller_->response_attachment_view();
    if (attachment.size > kMaxAttachmentBytes) {
      failure_code_ = kReplyTooLarge;
      co_return false;
    }

    const Status status =
        co_await conn_->RDMAWrite(attachment, request_->regions());
    if (!status.ok()) {
      failure_code_ = kReplyHandlerError;
      co_return false;
    }
  }

  SendBuffer* buffer =
      co_await conn_->AcquireSendBuffer(MessageType::kResponse);
  if (buffer == nullptr) {
    failure_code_ = kReplyHandlerError;
    co_return false;
  }

  buffer->length =
      Protocol::EncodeMessage(MessageBuffer{buffer->data, buffer->capacity},
                              {.type = MessageType::kResponse,
                               .opcode = request_->opcode(),
                               .code = kReplyOk,
                               .correlation_id = request_->correlation_id()},
                              &response);
  if (buffer->length == 0) {
    conn_->ReleaseSendBuffer(buffer, MessageType::kResponse);
    failure_code_ = kReplyTooLarge;
    co_return false;
  }

  const Status status = conn_->RDMASend(buffer);
  if (!status.ok()) {
    failure_code_ = kReplyHandlerError;
    co_return false;
  }

  response_sent_ = true;
  co_return true;
}

Session::Session(ConnectionUPtr conn, ServiceRegistry* service_registry,
                 std::function<Gate::Holder()> enter_gate)
    : conn_(std::move(conn)),
      service_registry_(service_registry),
      enter_gate_(std::move(enter_gate)) {}

Future<Status> Session::Start() {
  LOG(INFO) << "Session is starting...";

  EventHandler handler;
  handler.on_message_received = [this](ReceiveBuffer* buffer,
                                       const MessageView& message) {
    return OnMessageReceived(buffer, message);
  };
  const Status status = co_await conn_->Start(std::move(handler));
  if (!status.ok()) {
    co_return status;
  }

  running_ = true;
  LOG(INFO) << "Successfully start Session";
  co_return status;
}

Future<bool> Session::Shutdown() {
  if (!running_) {
    co_return co_await conn_->Shutdown();
  }

  LOG(INFO) << "Session is shutting down...";

  const bool succ = co_await conn_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown Session.";
  co_return succ;
}

Status Session::OnMessageReceived(ReceiveBuffer* buffer,
                                  const MessageView& message) {
  switch (message.type()) {
    case MessageType::kRequest:
      (void)ProcessRequest(buffer, message);
      return Status::OK();
    case MessageType::kResponse:
      CHECK(false) << "a response arrived at the serving end";
      return ToStatus(EPROTO, "dispatch an rdma message");
    default:
      LOG(ERROR) << "Fail to dispatch rdma message: unexpected type="
                 << static_cast<int>(message.header->type);
      conn_->ReleaseRecvBuffer(buffer);
      return ToStatus(EPROTO, "dispatch an rdma message");
  }
}

Future<> Session::ProcessRequest(ReceiveBuffer* buffer, MessageView message) {
  Gate::Holder holder = enter_gate_();
  if (holder.ok()) {
    const Status dispatch_status = co_await DispatchRequest(message);
    if (!dispatch_status.ok()) {
      LOG_EVERY_N(ERROR, 100) << "Fail to serve opcode " << message.opcode()
                              << ": " << dispatch_status.ToString();
    }
  }
  conn_->ReleaseRecvBuffer(buffer);
}

Future<Status> Session::DispatchRequest(const MessageView& message) {
  const Service::Method* method = service_registry_->Find(message.opcode());
  if (method == nullptr) {
    if (Alive()) {
      (void)co_await SendErrorResponse(message, kReplyBadOpcode);
    }
    co_return Status::OK();
  }

  Controller controller;
  const Status status = co_await ReadAttachment(message, &controller);
  if (!status.ok()) {
    co_return status;
  }

  ReplyWriter writer(conn_.get(), &message, &controller);
  const ReplyCode reply_code =
      co_await CallMethod(*method, &controller, message.payload, &writer);
  if (writer.response_sent()) {
    co_return Status::OK();
  }
  if (Alive()) {
    co_return co_await SendErrorResponse(message,
                                         writer.ResolveReplyCode(reply_code));
  }
  co_return Status::OK();
}

Future<Status> Session::ReadAttachment(const MessageView& message,
                                       Controller* controller) {
  const uint32_t attachment_size = message.attachment_size();
  if (attachment_size == 0) {
    co_return Status::OK();
  }

  Buffer attachment = Buffer::Alloc(attachment_size);
  if (attachment.empty()) {
    (void)co_await SendErrorResponse(message, kReplyTooLarge);
    co_return ToStatus(ENOMEM, "allocate an attachment buffer");
  }

  const Status status =
      co_await conn_->RDMARead(message.regions(), attachment.view());
  if (!status.ok()) {
    if (Alive()) {
      (void)co_await SendErrorResponse(message, kReplyHandlerError);
    }
    co_return status;
  }

  controller->request_attachment() = std::move(attachment);
  co_return Status::OK();
}

Future<ReplyCode> Session::CallMethod(const Service::Method& method,
                                      Controller* controller,
                                      std::string_view payload,
                                      ResponseWriter* writer) {
  co_return co_await method(controller, payload, writer);
}

Future<Status> Session::SendErrorResponse(const MessageView& request,
                                          ReplyCode reply_code) {
  SendBuffer* buffer =
      co_await conn_->AcquireSendBuffer(MessageType::kResponse);
  if (buffer == nullptr) {
    co_return ToStatus(ECONNRESET, "acquire a send buffer");
  }

  buffer->length =
      Protocol::EncodeMessage(MessageBuffer{buffer->data, buffer->capacity},
                              {.type = MessageType::kResponse,
                               .opcode = request.opcode(),
                               .code = reply_code,
                               .correlation_id = request.correlation_id()},
                              nullptr);
  DCHECK_GT(buffer->length, 0u) << "a send buffer too small for a header";
  co_return conn_->RDMASend(buffer);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
