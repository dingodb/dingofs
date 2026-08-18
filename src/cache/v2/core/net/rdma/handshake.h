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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_HANDSHAKE_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_HANDSHAKE_H_

#include <cstdint>
#include <cstring>

#include "cache/v2/core/net/rdma/option.h"
#include "cache/v2/core/net/rdma/verbs/buffer.h"
#include "cache/v2/core/net/rdma/verbs/device.h"
#include "cache/v2/core/net/rdma/verbs/queue_pair.h"

namespace dingofs {
namespace cache {
namespace v2 {

// The handshake: one rpc on whatever transport already serves the peer.
// The reply is the ready barrier: the responder is in RTS before answering.
// The structs ARE the wire format: packed, fixed-width, self-describing.

inline constexpr uint32_t kHandshakeMagic = 0x314e4344;  // "DCN1"
// 2: the tcp barrier message is gone, kPing exists, and `shard` is real.
inline constexpr uint16_t kHandshakeVersion = 2;

// A memory range this domain publishes for one-sided access.
struct __attribute__((packed)) ExposedRegion {
  uint64_t addr;
  uint64_t length;
  uint32_t rkey;
  uint32_t reserved;
};
static_assert(sizeof(ExposedRegion) == 24, "ExposedRegion is a wire structure");

struct __attribute__((packed)) HandshakeMsg {
  uint32_t magic;
  uint16_t version;
  // Echo of the sender's shard, proving the route hint landed as aimed.
  uint16_t shard;
  uint8_t num_qps;  // 1 small lane + N bulk lanes
  uint8_t link_layer;
  uint8_t rd_atomic;  // device cap; both sides take the min
  uint8_t reserved1;
  uint16_t rpc_credits;  // = my receive-ring depth
  uint16_t frame_bytes;  // = my receive-buffer size
  ExposedRegion exposed;
  verbs::QpPeer qps[1 + kMaxBulkLanes];
};
static_assert(sizeof(HandshakeMsg) == 40 + (32 * (1 + kMaxBulkLanes)),
              "HandshakeMsg is a wire structure");

// Returns nullptr when acceptable, else a reason suitable for logging.
inline const char* CheckHandshake(const HandshakeMsg& msg, uint8_t local_qps,
                                  verbs::LinkLayer local_link_layer) {
  if (msg.magic != kHandshakeMagic) {
    return "bad handshake magic";
  }
  if (msg.version != kHandshakeVersion) {
    return "handshake version mismatch";
  }
  if (msg.num_qps != local_qps) {
    return "peer advertises a different number of lanes";
  }
  if (msg.num_qps == 0 || msg.num_qps > 1 + kMaxBulkLanes) {
    return "peer advertises an invalid number of lanes";
  }
  if (static_cast<verbs::LinkLayer>(msg.link_layer) != local_link_layer) {
    return "link layer mismatch";
  }
  if (msg.rpc_credits == 0 || msg.frame_bytes == 0) {
    return "peer advertises no receive capacity";
  }
  return nullptr;
}

inline verbs::RemoteBuf RemoteOf(const ExposedRegion& region) {
  return verbs::RemoteBuf{region.addr, region.length, region.rkey};
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_HANDSHAKE_H_
