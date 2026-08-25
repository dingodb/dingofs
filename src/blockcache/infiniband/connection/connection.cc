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

#include "blockcache/infiniband/connection/connection.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/infiniband/base/buffer_pool.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(rdma_max_inflight_rpcs, 128, "max in-flight rpcs per connection");
DEFINE_validator(rdma_max_inflight_rpcs,
                 [](const char* /*name*/, uint32_t value) {
                   return infiniband::Protocol::IsValidInflightRpcs(value);
                 });

DEFINE_uint32(rdma_message_bytes, 16 << 10, "bytes in one message");
DEFINE_validator(rdma_message_bytes, [](const char* /*name*/, uint32_t value) {
  return value >= infiniband::Protocol::MessageSize(
                      infiniband::Protocol::kMaxRegions, 0) &&
         value <= UINT16_MAX;
});

DEFINE_uint32(rdma_max_connections, 64, "max rdma connections per shard");
DEFINE_validator(rdma_max_connections,
                 [](const char* /*name*/, uint32_t value) {
                   return value > 0;
                 });

DEFINE_uint32(rdma_heartbeat_interval_s, 2, "seconds between heartbeats");
DEFINE_validator(rdma_heartbeat_interval_s,
                 [](const char* /*name*/, uint32_t value) {
                   return value > 0;
                 });

DEFINE_uint32(rdma_idle_timeout_s, 10,
              "seconds before an idle connection is reaped");
DEFINE_validator(rdma_idle_timeout_s, [](const char* /*name*/, uint32_t value) {
  return value >= 3 * FLAGS_rdma_heartbeat_interval_s;
});

namespace infiniband {

static bool IsUnexpectedWc(int32_t wc_status, bool closing) {
  return wc_status != IBV_WC_SUCCESS &&
         (wc_status != IBV_WC_WR_FLUSH_ERR || !closing);
}

Connection::Connection(QueuePairGroup qps, BufferPool* buffer_pool,
                       ConnectionOption option)
    : option_(option),
      qps_(std::make_unique<QueuePairGroup>(std::move(qps))),
      recv_buffers_(std::make_unique<ReceiveBufferPool>(buffer_pool)),
      send_buffers_(std::make_unique<SendBufferPool>(buffer_pool)),
      receiver_(std::make_unique<Receiver>(qps_.get(), recv_buffers_.get())),
      msg_sender_(std::make_unique<MsgSender>(qps_.get(), send_buffers_.get())),
      bulk_sender_(std::make_unique<BulkSender>(qps_.get())),
      credit_flow_control_(std::make_unique<CreditFlowControl>(
          option.max_send_credits,
          static_cast<uint16_t>(Protocol::MsgRecvWr()))),
      heartbeat_timer_(std::make_unique<Timer>()),
      last_heard_ns_(CachedTimestampNs()) {}

Connection::~Connection() {
  CHECK(inflights() == 0 && unreleased_recv_buffers_ == 0)
      << "connection destroyed with " << inflights()
      << " work requests still outstanding and " << unreleased_recv_buffers_
      << " receive buffers still unreleased";
}

Future<Status> Connection::Start(EventHandler handler) {
  CHECK(handler.on_message_received) << "a connection dispatches nothing";
  handler_ = std::move(handler);

  LOG(INFO) << "Connection{qpn=" << msg_qpn() << "} is starting...";

  Status status = send_buffers_->Init(FLAGS_rdma_message_bytes,
                                      Protocol::SendBufferCount(), this);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init send buffers: " << status.ToString();
    co_return status;
  }

  status = recv_buffers_->Init(FLAGS_rdma_message_bytes, Protocol::MsgRecvWr(),
                               this);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init receive buffers: " << status.ToString();
    co_return status;
  }

  running_ = true;
  receiver_->Start();
  receiver_->PostAllWorkRequests();
  if (option_.heartbeat_interval_ns > 0) {
    StartHeartbeat();
  }

  LOG(INFO) << "Successfully start Connection{qpn=" << msg_qpn()
            << " qps=" << static_cast<int>(qps_->qp_count())
            << " message_bytes=" << FLAGS_rdma_message_bytes
            << " recv_buffers=" << recv_buffers_->buffer_count()
            << " max_send_credits=" << option_.max_send_credits << "}";
  co_return Status::OK();
}

Future<bool> Connection::Shutdown() {
  if (!running_) {
    co_await Drain();
    co_return unreleased_recv_buffers_ == 0;
  }

  LOG(INFO) << "Connection{qpn=" << msg_qpn() << "} is shutting down...";
  running_ = false;
  heartbeat_timer_->Cancel();
  SetError(ToStatus(ECANCELED, "keep the connection open"));

  co_await Drain();

  LOG(INFO) << "Successfully shutdown Connection{qpn=" << msg_qpn()
            << " unreleased_recv_buffers=" << unreleased_recv_buffers_ << "}";
  co_return unreleased_recv_buffers_ == 0;
}

void Connection::StartHeartbeat() {
  heartbeat_timer_->SetCallback([this] { SendHeartbeat(); });
  heartbeat_timer_->ArmPeriodic(
      std::chrono::nanoseconds(option_.heartbeat_interval_ns));
}

void Connection::SendHeartbeat() {
  if (!running_ || has_error_) {
    return;
  } else if (CachedTimestampNs() - last_sent_ns_ <
             option_.heartbeat_interval_ns) {
    return;
  }

  SendBuffer* buffer = send_buffers_->TryAcquire();
  if (buffer == nullptr) {
    return;
  }

  if (!credit_flow_control_->TryAcquireSendCredit()) {
    send_buffers_->Release(buffer);
    return;
  }
  (void)SendControl(buffer);
}

Future<> Connection::Drain() {
  static constexpr uint64_t kDrainReportNs = 5ull * 1000 * 1000 * 1000;

  uint64_t next_report = TimestampNs() + kDrainReportNs;
  while (inflights() > 0) {
    co_await Yield();
    if (TimestampNs() >= next_report) {
      LOG(ERROR) << "Fail to drain rdma connection in time, recv="
                 << receiver_->inflights()
                 << " msg=" << msg_sender_->inflights()
                 << " bulk=" << bulk_sender_->inflights()
                 << " qps=" << static_cast<int>(qps_->qp_count());
      next_report += kDrainReportNs;
    }
  }
}

Future<SendBuffer*> Connection::AcquireSendBuffer(MessageType type) {
  if (type == MessageType::kRequest &&
      !co_await credit_flow_control_->AcquireSendCredit()) {
    co_return nullptr;
  }

  SendBuffer* buffer = co_await send_buffers_->Acquire();
  if (buffer == nullptr && type == MessageType::kRequest) {
    credit_flow_control_->AddSendCredits(1);
  }
  co_return buffer;
}

void Connection::ReleaseSendBuffer(SendBuffer* buffer, MessageType type) {
  if (type == MessageType::kRequest) {
    credit_flow_control_->AddSendCredits(1);
  }

  buffer->length = 0;
  send_buffers_->Release(buffer);
}

void Connection::ReleaseRecvBuffer(ReceiveBuffer* buffer) {
  DCHECK_GT(unreleased_recv_buffers_, 0u) << "a receive buffer released twice";
  --unreleased_recv_buffers_;

  receiver_->RepostWorkRequest(buffer);
  credit_flow_control_->RestoreCreditsToReturn(1);
  ReturnCreditsIfNeeded();
}

Status Connection::RDMASend(SendBuffer* buffer) {
  DCHECK_GE(buffer->length, sizeof(MessageHeader))
      << "post a message that was never encoded";

  const uint16_t credits_to_return =
      credit_flow_control_->TakeCreditsToReturn();
  Protocol::SetCredit(MessageBuffer{buffer->data, buffer->length},
                      credits_to_return);

  const Status status = msg_sender_->Send(buffer, buffer->length);
  if (status.ok()) {
    last_sent_ns_ = CachedTimestampNs();
  } else {
    credit_flow_control_->RestoreCreditsToReturn(credits_to_return);
    SetError(status);
    buffer->length = 0;
    send_buffers_->Release(buffer);
  }
  return status;
}

Future<Status> Connection::RDMARead(std::span<const RemoteRegion> src,
                                    BufferView dst) {
  return bulk_sender_->Read(src, dst);
}

Future<Status> Connection::RDMAWrite(BufferView src,
                                     std::span<const RemoteRegion> dst) {
  return bulk_sender_->Write(src, dst);
}

void Connection::OnMessageReceived(ReceiveBuffer* buffer, const ibv_wc& wc) {
  receiver_->Countdown();

  if (wc.status != IBV_WC_SUCCESS) {
    if (IsUnexpectedWc(wc.status, !running_)) {
      SetError(ToStatus(wc.status, "receive rdma message"));
    }
    return;
  }

  last_heard_ns_ = CachedTimestampNs();

  MessageView message;
  const char* reason =
      Protocol::DecodeMessage(BufferView(buffer->data, wc.byte_len), &message);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to decode rdma message: " << reason;
    SetError(ToStatus(EPROTO, "receive an rdma message"));
    return;
  }

  if (message.credit() > 0) {
    credit_flow_control_->AddSendCredits(message.credit());
  }

  ++unreleased_recv_buffers_;

  if (message.type() == MessageType::kControl) {
    ReleaseRecvBuffer(buffer);
    return;
  }

  const Status status = handler_.on_message_received(buffer, message);
  if (!status.ok()) {
    SetError(status);
  }
}

void Connection::OnMessageSent(SendBuffer* buffer, int wc_status) {
  msg_sender_->Countdown();
  send_buffers_->Release(buffer);
  if (IsUnexpectedWc(wc_status, !running_)) {
    SetError(ToStatus(static_cast<ibv_wc_status>(wc_status), "send a message"));
  }
}

void Connection::ReturnCreditsIfNeeded() {
  if (return_credit_inflight_ || has_error_ || !running_) {
    return;
  }
  if (!credit_flow_control_->TryAcquireForCreditReturn()) {
    return;
  }
  return_credit_inflight_ = true;
  (void)SendCreditReturn();
}

Future<Status> Connection::SendCreditReturn() {
  SendBuffer* buffer = co_await send_buffers_->Acquire();
  Status status = ToStatus(ECONNRESET, "acquire a send buffer");
  if (buffer != nullptr) {
    status = SendControl(buffer);
  }
  return_credit_inflight_ = false;
  co_return status;
}

Status Connection::SendControl(SendBuffer* buffer) {
  buffer->length = Protocol::EncodeMessage(
      MessageBuffer{buffer->data, buffer->capacity},
      {.type = MessageType::kControl}, /*body=*/nullptr);
  DCHECK_GT(buffer->length, 0u) << "a send buffer too small for a header";
  return RDMASend(buffer);
}

void Connection::SetError(const Status& status) {
  if (has_error_) {
    return;
  }

  has_error_ = true;

  receiver_->Shutdown();
  qps_->ModifyToError();
  credit_flow_control_->Shutdown();
  send_buffers_->FailAll();

  if (running_) {
    LOG(WARNING) << "Fail to keep rdma connection alive: " << status.ToString();
  }

  if (handler_.on_error) {
    handler_.on_error(status);
  }
}

void Connection::FailIfIdle(uint64_t idle_ns) {
  DCHECK(option_.heartbeat_interval_ns == 0)
      << "the dialing end heartbeats on idle and is never judged by silence";
  if (Alive() && CachedTimestampNs() - last_heard_ns_ >= idle_ns) {
    SetError(ToStatus(ETIMEDOUT, "hear from the peer"));
  }
}

uint32_t Connection::msg_qpn() const {
  return qps_->GetMsgQueuePair()->get()->qp_num;
}

uint32_t Connection::inflights() const {
  return receiver_->inflights() + msg_sender_->inflights() +
         bulk_sender_->inflights();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
