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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_CONNECTION_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_CONNECTION_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <span>
#include <string_view>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/net/client.h"
#include "blockcache/net/handler.h"
#include "blockcache/net/infiniband/queue_pair.h"
#include "blockcache/net/rdma/handshake.h"
#include "blockcache/net/rdma/lane_set.h"
#include "blockcache/net/rdma/recv_ring.h"
#include "blockcache/net/rdma/rpc/call_table.h"
#include "blockcache/net/rdma/rpc/credit_gate.h"
#include "blockcache/net/rdma/rpc/frame.h"
#include "blockcache/net/rdma/rpc/frame_pool.h"
#include "blockcache/net/rdma/rpc/messenger.h"
#include "blockcache/net/rdma/rpc/request.h"
#include "blockcache/net/rdma/send_queue.h"
#include "blockcache/utils/gate.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

class RdmaConnection;
class RdmaDomain;

// Hands a consumed receive slot back to the ring; a Reply's releaser.
void ReleaseRecvSlot(void* conn, uint64_t recv_slot);

// Builds the reply a caller sees out of the receive slot it arrived in.
inline Reply MakeReply(RdmaConnection* conn, uint16_t recv_slot, ReplyCode code,
                       std::string_view payload) {
  return Reply(code, payload, &ReleaseRecvSlot, conn, recv_slot);
}

// Flush completions during an orderly close are the close, not failures.
inline bool IsRealError(int32_t wc_status, bool closing) {
  return wc_status != IBV_WC_SUCCESS &&
         (wc_status != IBV_WC_WR_FLUSH_ERR || !closing);
}

// ONE class for both ends: after RTS an RC queue pair is symmetric.
// Owned by its domain, pinned to one shard, not thread-safe by design.
class RdmaConnection final : public Client {
 public:
  explicit RdmaConnection(RdmaDomain* domain);
  ~RdmaConnection() override;

  RdmaConnection(const RdmaConnection&) = delete;
  RdmaConnection& operator=(const RdmaConnection&) = delete;

  // Creates the QPs, receive ring and frame pool; each step can fail.
  Status Init();

  // Moves QPs to ERROR and waits for every posted WR to flush back.
  Future<> Shutdown();
  Future<> Drain();

  // One-sided data plane: co_await conn->Write(local, remote).
  OpAwaiter Write(verbs::LocalBuf local, const verbs::RemoteBuf& remote);
  OpAwaiter Read(verbs::LocalBuf local, const verbs::RemoteBuf& remote);

  // Chains ops behind one completion; always on a bulk lane, never the small.
  OpBatch Batch();

  // Sends a request; `regions` publishes local memory for one-sided access.
  Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload,
                               std::span<const RegionDesc> regions,
                               BodyShape shape = BodyShape::kNone) {
    return messenger_.Call(opcode, payload, regions, shape);
  }

  // `route_hint` is ignored: on rdma the caller already chose its shard.
  Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload,
                               Body body, uint64_t route_hint) override;

  // The overloads above would otherwise hide the base's short forms.
  using Client::Call;

  // FillHandshake describes this side; ApplyPeer is the only RTR/RTS path.
  void FillHandshake(HandshakeMsg* msg, const ExposedRegion& exposed);
  Status ApplyPeer(const HandshakeMsg& msg);

  // Initiator only: pings feed the responder's idle sweep; RDMA has no FIN.
  void StartKeepalive(uint64_t interval_ns);

  // First failure wins; later ones are recorded but do not overwrite it.
  void OnError(Status status);

  // Completion routing, called by the CQ poller.
  void OnRecvWc(const ibv_wc& wc, RecvSlot* slot);

  // Send completion of a frame buffer: the only recycle point.
  void OnFrameWc(FrameBuf* buffer, int32_t wc_status) {
    messenger_.OnFrameWc(buffer, wc_status);
  }

  // Sum over the receive ring and every send queue. Drain waits for zero.
  uint32_t OutstandingWrs() const;

  // The idle sweep looks at accepted connections only.
  bool initiator() const { return initiator_; }
  uint64_t last_heard_ns() const { return last_heard_ns_; }
  bool alive() const { return alive_; }
  // True once Shutdown/Drain began: flush completions are then expected.
  bool closing() const { return closing_; }
  // Set on Shutdown() entry; only the caller that drove it may act on it.
  bool shutdown_done() const { return shutdown_done_; }
  // Membership flag for the domain's "needs replenish" list.
  bool recv_dirty() const { return recv_dirty_; }
  void set_recv_dirty(bool dirty) { recv_dirty_ = dirty; }
  const verbs::RemoteBuf& peer_region() const { return peer_region_; }
  uint32_t peer_credits() const { return peer_credits_; }
  // The shard the peer says it serves; check against what was aimed at.
  uint16_t peer_shard() const { return peer_shard_; }
  RdmaDomain* domain() const { return domain_; }
  // The domain's gate, so a detached dispatch coroutine can hold it.
  Gate* endpoint_gate();
  // The handler table this connection serves (its shard's).
  RequestHandler& handler();
  // The message layer: slots, credits, frames, dispatch.
  Messenger& messenger() { return messenger_; }
  LaneSet& lanes() { return lanes_; }

 private:
  friend class RdmaRequest;

  RdmaDomain* domain_;

  // Declaration order is teardown order: messenger_ dies before lanes_.
  LaneSet lanes_;
  Messenger messenger_;

  Timer ping_timer_;

  verbs::RemoteBuf peer_region_;
  uint32_t peer_credits_ = 0;
  uint16_t peer_shard_ = 0;
  uint64_t last_heard_ns_ = 0;
  bool initiator_ = false;
  bool alive_ = false;
  bool closing_ = false;
  bool shutdown_done_ = false;
  bool recv_dirty_ = false;
  Status error_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_CONNECTION_H_
