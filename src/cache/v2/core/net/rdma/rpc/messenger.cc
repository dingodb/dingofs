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

#include "cache/v2/core/net/rdma/rpc/messenger.h"

#include <glog/logging.h>

#include <cerrno>
#include <utility>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/connection.h"
#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/rpc/request.h"
#include "cache/v2/core/net/rdma/verbs/wc_status.h"
#include "cache/v2/core/net/rdma/wr_id.h"
#include "cache/v2/utils/time.h"

namespace dingofs {
namespace cache {
namespace v2 {

// `frame` by value: it points into the slot, posted until ReleaseRecv.
static Future<Status> DispatchWithGate(Gate* gate, RdmaConnection* conn,
                                       uint16_t recv_slot, FrameView frame) {
  Gate::Holder holder(*gate);
  if (!holder.ok()) {
    conn->messenger().ReleaseRecv(recv_slot);
    co_return Status::OK();  // shutting down: drop the request quietly
  }

  RdmaRequest request(conn, frame);
  const Status status = co_await conn->handler().Serve(request);
  conn->messenger().ReleaseRecv(recv_slot);
  co_return status;
}

static Future<Status> SendGrant(RdmaConnection* conn) {
  co_return co_await conn->messenger().SendCreditGrant();
}

void ReleaseRecvSlot(void* conn, uint64_t recv_slot) {
  static_cast<RdmaConnection*>(conn)->messenger().ReleaseRecv(
      static_cast<uint16_t>(recv_slot));
}

Status Messenger::Init(RdmaConnection* conn, RdmaDomain* domain) {
  conn_ = conn;
  const RdmaOption& option = domain->option();

  // Recv buffers must be posted while the queue pair is still in INIT.
  DINGOFS_RETURN_NOT_OK(recv_.Init(
      conn->lanes().small_qp().get(), conn, &domain->pool(),
      domain->memory().pool_lkey(),
      static_cast<uint16_t>(option.small_recv_wr()), option.recv_buf_bytes));
  recv_.PostAll();

  slots_.Init(option.max_inflight_rpcs);
  // Same budget as the receive ring (see RdmaOption::frame_budget).
  DINGOFS_RETURN_NOT_OK(
      frames_.Init(conn, &domain->pool(), domain->memory().pool_lkey(),
                   option.frame_pool_size(), option.recv_buf_bytes));
  ping_interval_ns_ = option.ping_interval_ns;
  return Status::OK();
}

Future<StatusOr<Reply>> Messenger::Call(Opcode opcode, std::string_view payload,
                                        std::span<const RegionDesc> regions,
                                        BodyShape shape) {
  // Validate before taking a slot or a credit; failing after strands both.
  if (regions.size() > kMaxRegions) {
    co_return ToStatus(EINVAL, "send a request: too many regions");
  }
  if (FrameSize(static_cast<uint8_t>(regions.size()), payload.size()) >
      frames_.frame_bytes()) {
    co_return ToStatus(EMSGSIZE, "send a request: it exceeds one frame");
  }
  if (!conn_->alive()) {
    co_return ToStatus(ENOTCONN, "send a request: connection is down");
  }

  auto acquire = slots_.AcquireEntry();
  const uint16_t slot = co_await acquire;
  if (acquire.failed()) {
    co_return ToStatus(ECONNRESET, "acquire a request slot");
  }
  if (!conn_->alive()) {
    slots_.Release(slot);
    co_return ToStatus(ECONNRESET, "acquire a request slot");
  }

  // Take the future before sending: the reply may arrive on the next poll.
  Future<StatusOr<Reply>> reply = slots_[slot].promise.GetFuture();

  co_await credits_.ConsumeOne();
  if (!conn_->alive()) {
    slots_.Release(slot);
    co_return ToStatus(ECONNRESET, "acquire a send credit");
  }

  Status sent =
      co_await SendFrame(FrameType::kRequest, shape, opcode, kReplyOk,
                         slots_.CorrelationOf(slot), regions, payload);
  if (!sent.ok()) {
    co_return sent;
  }
  co_return co_await std::move(reply);
}

Future<Status> Messenger::SendFrame(FrameType type, BodyShape shape,
                                    uint16_t opcode, ReplyCode code,
                                    uint64_t correlation,
                                    std::span<const RegionDesc> regions,
                                    std::string_view payload) {
  FrameBuf* buffer = frames_.TryGet();
  if (buffer == nullptr) {
    return SendFrameSlow(type, shape, opcode, code, correlation, regions,
                         payload);
  }
  PostFrame(buffer, type, shape, opcode, code, correlation, regions, payload);
  return MakeReadyFuture<Status>(Status::OK());
}

void Messenger::FailAll(const Status& status) {
  slots_.FailAll(status);
  credits_.FailAll();
  frames_.FailAll();
}

Future<Status> Messenger::SendFrameSlow(FrameType type, BodyShape shape,
                                        uint16_t opcode, ReplyCode code,
                                        uint64_t correlation,
                                        std::span<const RegionDesc> regions,
                                        std::string_view payload) {
  FrameBuf* buffer = co_await frames_.GetBuf();
  if (buffer == nullptr) {
    co_return ToStatus(ECONNRESET, "acquire a frame buffer");
  }
  PostFrame(buffer, type, shape, opcode, code, correlation, regions, payload);
  co_return Status::OK();
}

void Messenger::PostFrame(FrameBuf* buffer, FrameType type, BodyShape shape,
                          uint16_t opcode, ReplyCode code, uint64_t correlation,
                          std::span<const RegionDesc> regions,
                          std::string_view payload) {
  FrameHeader header{};
  header.magic = kFrameMagic;
  header.type = static_cast<uint8_t>(type);
  header.body_shape = static_cast<uint8_t>(shape);
  header.region_count = static_cast<uint8_t>(regions.size());
  header.opcode = opcode;
  header.payload_len = static_cast<uint32_t>(payload.size());
  header.credit = TakePiggyback();
  header.code = code;
  header.correlation = correlation;

  const size_t len = EncodeFrame(buffer->frame, header, regions, payload);
  const verbs::LocalBuf frame{buffer->frame, static_cast<uint32_t>(len),
                              frames_.lkey()};
  if (!conn_->lanes().small_send().PostFrame(MakeWrId(buffer, kTagFrame), frame)) {
    // Credits bound outstanding frames; a full send queue = broken accounting.
    frames_.Put(buffer);
    conn_->OnError(ToStatus(ENOBUFS, "post a frame: send queue is full"));
    return;
  }
  last_sent_ns_ = CachedTimestampNs();
}

// Any frame proves liveness; ping only when quiet, skip when busy.
void Messenger::OnFrameWc(FrameBuf* buffer, int32_t wc_status) {
  conn_->lanes().small_send().OnFrameWc();
  frames_.Put(buffer);
  if (IsRealError(wc_status, conn_->closing())) {
    conn_->OnError(
        verbs::WcStatus(static_cast<ibv_wc_status>(wc_status), "send a frame"));
  }
}

void Messenger::ReleaseRecv(uint16_t recv_slot) {
  recv_.Recycle(recv_slot);
  conn_->domain()->poller().MarkDirtyRecv(conn_);
  ++pending_credit_return_;
  MaybeSendGrant();
}

void Messenger::MaybeSendPing() {
  if (!conn_->alive() || conn_->closing()) {
    return;
  }
  if (CachedTimestampNs() - last_sent_ns_ < ping_interval_ns_) {
    return;
  }
  FrameBuf* buffer = frames_.TryGet();
  if (buffer == nullptr) {
    return;
  }
  if (!credits_.TryConsumeOne()) {
    frames_.Put(buffer);
    return;
  }
  PostFrame(buffer, FrameType::kPing, BodyShape::kNone, 0, kReplyOk, 0, {}, {});
}

uint16_t Messenger::TakePiggyback() {
  uint32_t n = pending_credit_return_;
  if (n > 0xffff) {
    n = 0xffff;
  }
  pending_credit_return_ -= n;
  return static_cast<uint16_t>(n);
}

// Credits normally ride on traffic; only a one-way burst needs a grant.
void Messenger::OnFrame(RecvSlot* slot, uint32_t len) {
  FrameView frame;
  const char* reason = DecodeFrame(slot->data, len, &frame);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to decode rdma frame: " << reason;
    ReleaseRecv(slot->idx);
    conn_->OnError(ToStatus(EPROTO, "decode an rdma frame"));
    return;
  }

  // Reclaim credits first: the sender may be parked waiting for them.
  if (frame.header->credit > 0) {
    credits_.Refill(frame.header->credit);
  }

  switch (static_cast<FrameType>(frame.header->type)) {
    case FrameType::kResponse: {
      auto* entry = slots_.Match(frame.header->correlation);
      if (entry == nullptr) {
        // Stale or duplicated: the generation says this slot has moved on.
        ReleaseRecv(slot->idx);
        break;
      }
      const uint16_t index = CorrelationSlot(frame.header->correlation);
      // The reply borrows the receive buffer until the caller drops it.
      entry->promise.SetValue(StatusOr<Reply>(
          MakeReply(conn_, slot->idx, frame.header->code, frame.payload)));
      slots_.Release(index);
      break;
    }
    case FrameType::kRequest:
      (void)DispatchWithGate(conn_->endpoint_gate(), conn_, slot->idx, frame);
      break;
    case FrameType::kCreditGrant:
    case FrameType::kPing:
      ReleaseRecv(slot->idx);
      break;
    default:
      LOG(ERROR) << "Fail to dispatch rdma frame: unknown type="
                 << static_cast<int>(frame.header->type);
      ReleaseRecv(slot->idx);
      conn_->OnError(ToStatus(EPROTO, "dispatch an rdma frame"));
      break;
  }
}

Future<Status> Messenger::SendCreditGrant() {
  Status status = co_await SendFrame(FrameType::kCreditGrant, BodyShape::kNone,
                                     0, kReplyOk, 0, {}, {});
  grant_in_flight_ = false;
  co_return status;
}

void Messenger::MaybeSendGrant() {
  if (grant_in_flight_ || !conn_->alive() || conn_->closing()) {
    return;
  }
  if (pending_credit_return_ * 2 < recv_.count()) {
    return;
  }
  if (!credits_.TakeReserveOrDead()) {
    return;
  }
  grant_in_flight_ = true;
  (void)SendGrant(conn_);
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
