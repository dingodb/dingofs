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

#include "cache/v2/core/net/rdma/connection.h"

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <utility>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/lane_set.h"
#include "cache/v2/core/net/rdma/verbs/wc_status.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/core/reactor/reactor.h"
#include "cache/v2/core/reactor/timer.h"

namespace dingofs {
namespace cache {
namespace v2 {

static constexpr uint64_t kDrainReportNs = 5ull * 1000 * 1000 * 1000;

RdmaConnection::RdmaConnection(RdmaDomain* domain) : domain_(domain) {}

Status RdmaConnection::Init() {
  DINGOFS_RETURN_NOT_OK(lanes_.Init(domain_));
  DINGOFS_RETURN_NOT_OK(messenger_.Init(this, domain_));
  return Status::OK();
}

RdmaConnection::~RdmaConnection() {
  // Destroying a QP with WRs outstanding lets the device write into freed
  // coroutine frames; Drain() guarantees this holds.
  CHECK(OutstandingWrs() == 0)
      << "connection destroyed with " << OutstandingWrs()
      << " work requests still outstanding";
}

OpAwaiter RdmaConnection::Write(verbs::LocalBuf local,
                                const verbs::RemoteBuf& remote) {
  return OpAwaiter(lanes_.NextBulk(), IBV_WR_RDMA_WRITE, local, remote.addr,
                   remote.rkey);
}

OpAwaiter RdmaConnection::Read(verbs::LocalBuf local,
                               const verbs::RemoteBuf& remote) {
  return OpAwaiter(lanes_.NextBulk(), IBV_WR_RDMA_READ, local, remote.addr,
                   remote.rkey);
}

OpBatch RdmaConnection::Batch() { return OpBatch(lanes_.NextBulk()); }

void RdmaConnection::FillHandshake(HandshakeMsg* msg,
                                   const ExposedRegion& exposed) {
  const verbs::PortInfo& port = domain_->device().port();
  const uint8_t rd_atomic = domain_->rd_atomic_cap();
  const RdmaOption& option = domain_->option();

  std::memset(msg, 0, sizeof(*msg));
  msg->magic = kHandshakeMagic;
  msg->version = kHandshakeVersion;
  msg->shard = static_cast<uint16_t>(HasReactor() ? ThisShardId() : 0);
  msg->num_qps = lanes_.count();
  msg->link_layer = static_cast<uint8_t>(port.link_layer);
  msg->rd_atomic = rd_atomic;
  msg->rpc_credits = static_cast<uint16_t>(option.small_recv_wr());
  msg->frame_bytes = static_cast<uint16_t>(option.recv_buf_bytes);
  msg->exposed = exposed;

  lanes_.FillPeer(port, rd_atomic, msg);
}

// The only place a QP moves to RTR then RTS; same code on both ends.
Future<> RdmaConnection::Shutdown() {
  if (shutdown_done_) {
    co_return;
  }
  shutdown_done_ = true;
  closing_ = true;

  ping_timer_.Cancel();
  co_await Drain();
}

Future<> RdmaConnection::Drain() {
  if (alive_) {
    closing_ = true;
    OnError(ToStatus(ECANCELED, "keep the connection open"));
  }

  // Bounded: QPs in ERROR flush every posted WR; never-posted ones were
  // already failed by SendQueue::SetBroken.
  uint64_t next_report = TimestampNs() + kDrainReportNs;
  while (OutstandingWrs() > 0) {
    co_await Yield();
    if (TimestampNs() >= next_report) {
      LOG(ERROR) << "Fail to drain rdma connection in time, recv="
                 << messenger_.recv().outstanding()
                 << " send=" << lanes_.OutstandingWrs()
                 << " lanes=" << static_cast<int>(lanes_.count());
      next_report += kDrainReportNs;
    }
  }
}

Status RdmaConnection::ApplyPeer(const HandshakeMsg& msg) {
  const verbs::PortInfo& port = domain_->device().port();
  const verbs::QpOption option = SmallLaneOption(domain_->option());

  // Both sides must agree on the read window, so take the smaller cap.
  const uint8_t rd_atomic = std::min(domain_->rd_atomic_cap(), msg.rd_atomic);

  DINGOFS_RETURN_NOT_OK(lanes_.ApplyPeer(msg, port, option, rd_atomic));

  peer_region_ = RemoteOf(msg.exposed);
  peer_credits_ = msg.rpc_credits;
  peer_shard_ = msg.shard;
  // One credit per receive buffer the peer posted; known only now.
  messenger_.SetPeerCredits(msg.rpc_credits);
  // The idle clock starts here.
  last_heard_ns_ = CachedTimestampNs();
  alive_ = true;
  return Status::OK();
}

void RdmaConnection::StartKeepalive(uint64_t interval_ns) {
  initiator_ = true;
  ping_timer_.SetCallback([this] { messenger_.MaybeSendPing(); });
  ping_timer_.ArmPeriodic(std::chrono::nanoseconds(interval_ns));
}

void RdmaConnection::OnRecvWc(const ibv_wc& wc, RecvSlot* slot) {
  messenger_.recv().OnWcArrived();
  if (wc.status != IBV_WC_SUCCESS) {
    if (IsRealError(wc.status, closing_)) {
      OnError(verbs::WcStatus(wc.status, "receive rdma message"));
    }
    return;
  }
  // Cached: refreshed by the batch that delivered this very completion.
  last_heard_ns_ = CachedTimestampNs();
  // A frame is recognised by its magic.
  if (wc.byte_len >= sizeof(FrameHeader) &&
      *reinterpret_cast<const uint32_t*>(slot->data) == kFrameMagic) {
    messenger_.OnFrame(slot, wc.byte_len);
    return;
  }
  messenger_.ReleaseRecv(slot->idx);
}

uint32_t RdmaConnection::OutstandingWrs() const {
  return messenger_.recv().outstanding() + lanes_.OutstandingWrs();
}

RequestHandler& RdmaConnection::handler() { return domain_->handler(); }

Gate* RdmaConnection::endpoint_gate() { return &domain_->gate(); }

void RdmaConnection::OnError(Status status) {
  if (!error_.ok()) {
    return;  // first failure wins
  }
  error_ = std::move(status);
  alive_ = false;

  // ERROR flushes posted WRs (Drain terminates) and fences late one-sided
  // writes out of buffers about to be handed back.
  messenger_.recv().SetDead();
  lanes_.Break();

  // Wake everyone the message layer has parked.
  messenger_.FailAll(error_);

  if (!closing_) {
    LOG(WARNING) << "Fail to keep rdma connection alive: " << error_.ToString();
  }
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
