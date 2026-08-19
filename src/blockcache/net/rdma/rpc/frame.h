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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_RPC_FRAME_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_RPC_FRAME_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "blockcache/net/body.h"
#include "blockcache/net/infiniband/buffer.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {

// Wire format for the message lane: FrameHeader (32B) + RegionDesc*rc +
// payload. Bulk data never travels here; it moves through the advertised
// regions. Little-endian on the wire; asserted rather than byte-swapped.
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "the rdma wire format is little-endian");

inline constexpr uint32_t kFrameMagic = 0x324e4344;  // "DCN2"

enum class FrameType : uint8_t {
  kRequest = 1,
  kResponse = 2,
  kCreditGrant = 3,
  // One-way liveness proof, sent when nothing else is flowing.
  kPing = 4,
};

// Maximum one-sided regions a single message may advertise. A writer hands
// its body over as the pages it accumulated into, so this has to cover a
// whole block's worth of them: 4 MiB over 64 KiB pages is 64. The frame
// grows by 16 bytes per region and still fits recv_buf_bytes with room to
// spare (32 + 64*16 = 1056 against 16 KiB).
//
// WIRE CONSTANT: both ends must agree. v2 client and v2 node ship together.
inline constexpr uint8_t kMaxRegions = kMaxBufferViews;

// A remote range the sender publishes; the peer reads/writes it directly.
struct __attribute__((packed)) RegionDesc {
  uint64_t addr;
  uint32_t len;
  uint32_t rkey;
};
static_assert(sizeof(RegionDesc) == 16, "RegionDesc is a wire structure");

// Total bytes across a set of advertised regions.
inline uint64_t RegionBytes(std::span<const RegionDesc> regions) {
  uint64_t total = 0;
  for (const RegionDesc& region : regions) {
    total += region.len;
  }
  return total;
}

struct __attribute__((packed)) FrameHeader {
  uint32_t magic;      // ASCII "DCN2"
  uint8_t type;        // FrameType
  uint8_t body_shape;  // BodyShape: which way the advertised regions travel
  uint8_t region_count;
  uint8_t reserved0;
  uint16_t opcode;
  uint16_t credit;  // receive buffers the sender is returning
  ReplyCode code;   // responses only
  uint16_t reserved1;
  uint32_t payload_len;
  uint32_t reserved2;  // keeps correlation 8-aligned and the header at 32B
  uint64_t correlation;
};
static_assert(sizeof(FrameHeader) == 32, "FrameHeader is a wire structure");
static_assert(offsetof(FrameHeader, correlation) == 24,
              "FrameHeader must have no implicit padding");

// body_shape rides the wire so the receiver need not know the verb.
struct FrameView {
  const FrameHeader* header = nullptr;
  const RegionDesc* regions = nullptr;
  uint8_t region_count = 0;
  std::string_view payload;

  BodyShape body_shape() const {
    return static_cast<BodyShape>(header->body_shape);
  }

  // Total bytes across every advertised region.
  uint32_t body_bytes() const {
    return static_cast<uint32_t>(RegionBytes({regions, region_count}));
  }
};

inline size_t FrameSize(uint8_t region_count, uint32_t payload_len) {
  return sizeof(FrameHeader) + (size_t{region_count} * sizeof(RegionDesc)) +
         payload_len;
}

inline size_t EncodeFrame(char* dst, const FrameHeader& header,
                          std::span<const RegionDesc> regions,
                          std::string_view payload) {
  std::memcpy(dst, &header, sizeof(header));
  size_t at = sizeof(header);
  if (!regions.empty()) {
    std::memcpy(dst + at, regions.data(), regions.size() * sizeof(RegionDesc));
    at += regions.size() * sizeof(RegionDesc);
  }
  if (!payload.empty()) {
    std::memcpy(dst + at, payload.data(), payload.size());
  }
  return at + payload.size();
}

// Returns nullptr when well formed, else a reason; lengths validated first.
inline const char* DecodeFrame(const char* data, uint32_t len, FrameView* out) {
  if (len < sizeof(FrameHeader)) {
    return "frame shorter than its header";
  }
  const auto* header = reinterpret_cast<const FrameHeader*>(data);
  if (header->magic != kFrameMagic) {
    return "bad frame magic";
  }
  if (header->region_count > kMaxRegions) {
    return "too many regions";
  }
  if (header->body_shape > static_cast<uint8_t>(BodyShape::kToClient)) {
    return "unknown body shape";
  }
  const size_t expected = FrameSize(header->region_count, header->payload_len);
  if (expected > len) {
    return "frame longer than the bytes received";
  }

  out->header = header;
  out->region_count = header->region_count;
  out->regions =
      header->region_count == 0
          ? nullptr
          : reinterpret_cast<const RegionDesc*>(data + sizeof(FrameHeader));
  out->payload =
      std::string_view(data + sizeof(FrameHeader) +
                           (size_t{header->region_count} * sizeof(RegionDesc)),
                       header->payload_len);
  return nullptr;
}

// correlation = (generation << 16) | slot; generation drops late replies.
inline uint64_t MakeCorrelation(uint16_t slot, uint64_t generation) {
  return (generation << 16) | slot;
}
inline uint16_t CorrelationSlot(uint64_t correlation) {
  return static_cast<uint16_t>(correlation & 0xffff);
}
inline uint64_t CorrelationGeneration(uint64_t correlation) {
  return correlation >> 16;
}

inline verbs::RemoteBuf RemoteOf(const RegionDesc& region) {
  return verbs::RemoteBuf{
      .addr = region.addr, .len = region.len, .rkey = region.rkey};
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_RPC_FRAME_H_
