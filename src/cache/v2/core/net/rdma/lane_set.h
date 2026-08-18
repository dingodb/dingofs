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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_LANE_SET_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_LANE_SET_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cache/v2/core/net/rdma/handshake.h"
#include "cache/v2/core/net/rdma/option.h"
#include "cache/v2/core/net/rdma/send_queue.h"
#include "cache/v2/core/net/rdma/verbs/queue_pair.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RdmaDomain;

// Shared: ApplyPeer hands the same options back when moving a lane to RTS.
verbs::QpOption SmallLaneOption(const RdmaOption& option);

// The queue pairs of one connection and the state machine driving them.
// The small lane carries frames; N bulk lanes carry one-sided data.
class LaneSet {
 public:
  LaneSet() = default;

  LaneSet(const LaneSet&) = delete;
  LaneSet& operator=(const LaneSet&) = delete;

  // Creates every queue pair in INIT and its send queue; each step can fail.
  Status Init(RdmaDomain* domain);

  // Moves every lane to ERROR: flushes posted WRs; the rpc-timeout fence.
  void Break();  // QPs to ERROR, send queues broken

  // Describes every lane to the peer: qpn, psn, port addressing.
  void FillPeer(const verbs::PortInfo& port, uint8_t rd_atomic,
                HandshakeMsg* msg);

  // THE only place a QP moves to RTR then RTS; identical on both ends.
  Status ApplyPeer(const HandshakeMsg& msg, const verbs::PortInfo& port,
                   const verbs::QpOption& option, uint8_t rd_atomic);

  // Round-robin over the bulk lanes.
  SendQueue* NextBulk();

  uint32_t OutstandingWrs() const;

  SendQueue& small_send() { return *small_queue_; }
  verbs::QueuePair& small_qp() { return small_qp_; }
  uint8_t count() const { return static_cast<uint8_t>(1 + bulk_.size()); }

 private:
  struct BulkLane {
    verbs::QueuePair qp;
    std::unique_ptr<SendQueue> queue;
  };

  // Declaration order is teardown order: queue before QP, QP before the CQ.
  verbs::QueuePair small_qp_;
  std::unique_ptr<SendQueue> small_queue_;
  std::vector<BulkLane> bulk_;
  unsigned next_bulk_ = 0;
  std::vector<uint32_t> local_psn_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_LANE_SET_H_
