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

#include "blockcache/infiniband/client/channel.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/client/context.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Channel::Channel() : Channel(CHECK_NOTNULL(ThisInfinibandContext())) {}

Channel::Channel(InfinibandContext* context)
    : dialer_(std::make_unique<Dialer>(context->device,
                                       context->buffer_pool.get(),
                                       context->completion_queue.get())),
      registry_(CHECK_NOTNULL(ThisMemoryRegistry())) {
  inflight_rpcs_.Init(static_cast<uint16_t>(FLAGS_rdma_max_inflight_rpcs));
  handler_ = EventHandler{
      .on_message_received =
          [this](ReceiveBuffer* buffer, const MessageView& message) {
            return OnMessageReceived(buffer, message);
          },
      .on_error = [this](const Status& status) { FailAll(status); },
  };
}

Channel::~Channel() {
  CHECK(conn_ == nullptr) << "a channel was destroyed before Shutdown()";
}

Future<Status> Channel::Init(ChannelOption option) {
  CHECK(conn_ == nullptr) << "a channel dials once";

  option_ = std::move(option);
  LOG(INFO) << "RdmaChannel{server=" << option_.server
            << "} is initializing...";

  StatusOr<ConnectionUPtr> conn = co_await dialer_->Dial(option_);
  if (!conn.ok()) {
    co_return conn.status();
  }

  conn_ = std::move(conn).value();
  const Status status = co_await conn_->Start(handler_);
  if (!status.ok()) {
    co_await Shutdown();
    co_return status;
  }

  LOG(INFO) << "Successfully init RdmaChannel{server=" << option_.server << "}";
  co_return status;
}

Future<> Channel::Shutdown() {
  if (conn_ == nullptr) {
    co_return;
  }

  LOG(INFO) << "RdmaChannel{server=" << option_.server
            << "} is shutting down...";

  const bool at_rest = co_await conn_->Shutdown();
  CHECK(at_rest) << "a channel still lends out a receive slot after shutdown";
  conn_.reset();
  dialer_.reset();

  LOG(INFO) << "Successfully shutdown RdmaChannel{server=" << option_.server
            << "}";
}

Future<Status> Channel::CallMethod(blockcache::Call call) {
  if (conn_ == nullptr || !conn_->Alive()) {
    co_return ToStatus(ENOTCONN, "send a request: connection is down");
  }

  StatusOr<Attachment> attachment = ParseAttachment(call);
  if (!attachment.ok()) {
    co_return attachment.status();
  }

  // register response by correlation id
  StatusOr<uint16_t> slot = co_await AcquireRequestSlot(call.response);
  if (!slot.ok()) {
    co_return slot.status();
  }

  const uint16_t index = slot.value();
  Future<StatusOr<ReplyCode>> resp = inflight_rpcs_[index].promise.GetFuture();

  // send request
  const Status sent = co_await SendRequest(
      inflight_rpcs_.GetCorrelationId(index), call, attachment.value());
  if (!sent.ok()) {
    inflight_rpcs_.Fail(index, sent);
    co_return sent;
  }

  // handle response
  StatusOr<ReplyCode> code = co_await std::move(resp);
  if (!code.ok()) {
    co_return code.status();
  } else if (code.value() != kReplyOk) {
    co_return Status::Internal("rpc rejected: code=" +
                               std::to_string(code.value()));
  }
  co_return Status::OK();
}

StatusOr<Channel::Attachment> Channel::ParseAttachment(
    const blockcache::Call& call) {
  Attachment attachment;
  if (!call.send.empty()) {
    attachment.type = RegionType::kToServer;
    attachment.ranges = call.send;
  } else if (!call.recv.empty()) {
    attachment.type = RegionType::kToClient;
    attachment.ranges = BufferViews(&call.recv, 1);
  }
  if (attachment.ranges.size() > Protocol::kMaxRegions) {
    return ToStatus(E2BIG, "send a request: too many attachment ranges");
  }
  return attachment;
}

Future<StatusOr<uint16_t>> Channel::AcquireRequestSlot(
    google::protobuf::Message* response) {
  auto awaiter = inflight_rpcs_.AcquireSlot();
  const uint16_t index = co_await awaiter;
  if (awaiter.failed() || !conn_->Alive()) {
    if (!awaiter.failed()) {
      inflight_rpcs_.Release(index);
    }
    co_return ToStatus(ECONNRESET, "acquire a request slot");
  }

  inflight_rpcs_[index].response = response;
  co_return index;
}

Future<Status> Channel::SendRequest(uint64_t correlation_id,
                                    const blockcache::Call& call,
                                    const Attachment& attachment) {
  SendBuffer* buffer = co_await conn_->AcquireSendBuffer(MessageType::kRequest);
  if (buffer == nullptr) {
    co_return ToStatus(ECONNRESET, "acquire a send buffer");
  }

  const Status status = EncodeRequest(correlation_id, call, attachment, buffer);
  if (!status.ok()) {
    conn_->ReleaseSendBuffer(buffer, MessageType::kRequest);
    co_return status;
  }

  co_return conn_->RDMASend(buffer);
}

Status Channel::EncodeRequest(uint64_t correlation_id,
                              const blockcache::Call& call,
                              const Attachment& attachment,
                              SendBuffer* buffer) const {
  const MessageBuffer message{.data = buffer->data,
                              .capacity = buffer->capacity};
  StatusOr<uint8_t> region_count =
      AddRegions(Protocol::GetRegions(message), attachment.ranges);
  if (!region_count.ok()) {
    return region_count.status();
  }

  buffer->length =
      Protocol::EncodeMessage(message,
                              {.type = MessageType::kRequest,
                               .region_type = attachment.type,
                               .opcode = call.opcode,
                               .correlation_id = correlation_id,
                               .region_count = region_count.value()},
                              call.request);
  if (buffer->length == 0) {
    return ToStatus(EMSGSIZE, "send a request: it exceeds one message");
  }
  return Status::OK();
}

StatusOr<uint8_t> Channel::AddRegions(RemoteRegion* regions,
                                      BufferViews ranges) const {
  uint8_t count = 0;
  for (const BufferView& range : ranges) {
    if (range.empty()) {
      return ToStatus(EINVAL, "send a request: an empty attachment range");
    }

    StatusOr<uint32_t> rkey = registry_->GetRKey(range.data, range.size);
    if (!rkey.ok()) {
      return ToStatus(EINVAL,
                      "send a request: the attachment is not registered");
    }

    regions[count++] =
        RemoteRegion{.addr = reinterpret_cast<uint64_t>(range.data),
                     .len = range.size,
                     .rkey = rkey.value()};
  }
  return count;
}

Status Channel::OnMessageReceived(ReceiveBuffer* buffer,
                                  const MessageView& message) {
  Status status = Status::OK();
  switch (message.type()) {
    case MessageType::kResponse:
      OnResponseReceived(message);
      break;
    case MessageType::kRequest:
      CHECK(false) << "a request arrived at the dialing end";
      break;
    default:
      LOG(ERROR) << "Fail to dispatch rdma message: unexpected type="
                 << static_cast<int>(message.header->type);
      status = ToStatus(EPROTO, "dispatch an rdma message");
      break;
  }
  conn_->ReleaseRecvBuffer(buffer);
  return status;
}

void Channel::OnResponseReceived(const MessageView& message) {
  auto* slot = inflight_rpcs_.FindByCorrelationId(message.correlation_id());
  if (slot == nullptr) {
    return;
  }

  StatusOr<ReplyCode> resp_code(message.code());
  if (message.accepted() &&
      !slot->response->ParseFromArray(
          message.payload.data(), static_cast<int>(message.payload.size()))) {
    resp_code = Status::Internal("fail to decode the rpc reply");
  }
  slot->promise.SetValue(std::move(resp_code));
  inflight_rpcs_.ReleaseByCorrelationId(message.correlation_id());
}

void Channel::FailAll(const Status& error) { inflight_rpcs_.FailAll(error); }

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
