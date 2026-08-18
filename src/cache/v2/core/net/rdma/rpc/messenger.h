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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_MESSENGER_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_MESSENGER_H_

#include <cstdint>
#include <span>
#include <string_view>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/body.h"
#include "cache/v2/core/net/client.h"
#include "cache/v2/core/net/rdma/recv_ring.h"
#include "cache/v2/core/net/rdma/rpc/call_table.h"
#include "cache/v2/core/net/rdma/rpc/credit_gate.h"
#include "cache/v2/core/net/rdma/rpc/frame.h"
#include "cache/v2/core/net/rdma/rpc/frame_pool.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RdmaConnection;
class RdmaDomain;

// The message layer of a connection: slots, credits, frames, dispatch.
// Held by value inside RdmaConnection: no indirection on the request path.
class Messenger {
 public:
  Messenger() = default;

  Messenger(const Messenger&) = delete;
  Messenger& operator=(const Messenger&) = delete;

  Status Init(RdmaConnection* conn, RdmaDomain* domain);

  // One credit per receive buffer the peer posted; known after handshake.
  void SetPeerCredits(uint16_t peer_credits) { credits_.Init(peer_credits); }

  // Sends a request, resolves with the reply; back pressure: slot, then
  // credit.
  Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload,
                               std::span<const RegionDesc> regions,
                               BodyShape shape);

  // Not a coroutine: with a buffer in hand the send is synchronous.
  Future<Status> SendFrame(FrameType type, BodyShape shape, uint16_t opcode,
                           ReplyCode code, uint64_t correlation,
                           std::span<const RegionDesc> regions,
                           std::string_view payload);

  // A frame arrived in `slot`, `len` bytes of it.
  void OnFrame(RecvSlot* slot, uint32_t len);

  // Send completion: the only point at which the buffer may be recycled.
  void OnFrameWc(FrameBuf* buffer, int32_t wc_status);

  // Returns a consumed receive buffer and remembers a credit owed to peer.
  void ReleaseRecv(uint16_t recv_slot);

  // Sends a liveness ping if quiet; synchronous, skips when busy.
  void MaybeSendPing();

  // On timeout/death: wakes everyone parked on a slot, credit or frame buffer.
  void FailAll(const Status& status);

  // Spawned plumbing; public because the coroutine runs outside the class.
  Future<Status> SendCreditGrant();

  RecvRing& recv() { return recv_; }
  const RecvRing& recv() const { return recv_; }

 private:
  Future<Status> SendFrameSlow(FrameType type, BodyShape shape, uint16_t opcode,
                               ReplyCode code, uint64_t correlation,
                               std::span<const RegionDesc> regions,
                               std::string_view payload);
  void PostFrame(FrameBuf* buffer, FrameType type, BodyShape shape,
                 uint16_t opcode, ReplyCode code, uint64_t correlation,
                 std::span<const RegionDesc> regions, std::string_view payload);
  // Credits ride back on any outgoing frame; only a burst needs a grant.
  uint16_t TakePiggyback();
  void MaybeSendGrant();

  RdmaConnection* conn_ = nullptr;

  RecvRing recv_;
  CallTable<StatusOr<Reply>> slots_;
  CreditGate credits_;
  FramePool frames_;
  uint32_t pending_credit_return_ = 0;
  bool grant_in_flight_ = false;
  uint64_t last_sent_ns_ = 0;  // any frame counts as a liveness proof
  uint64_t ping_interval_ns_ = 0;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_MESSENGER_H_
