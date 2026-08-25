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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_PROTOCOL_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_PROTOCOL_H_

#include <google/protobuf/message.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/net/types.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "the rdma protocol is little-endian");

inline constexpr uint8_t kMaxBulkQps = 16;

enum class RegionType : uint8_t {
  kNone = 0,
  kToServer = 1,
  kToClient = 2,
};

enum class MessageType : uint8_t {
  kRequest = 1,
  kResponse = 2,
  kControl = 3,
};

struct HandshakeMsg {
  uint16_t version;
  uint16_t shard;
  uint8_t num_qps;  // 1 message QP + N bulk QPs
  uint8_t link_layer;
  uint16_t rpc_credits;    // = my receive-ring depth
  uint16_t message_bytes;  // = my receive-buffer size
  QueuePairInfo qp_infos[1 + kMaxBulkQps];

  void ToPb(pb::blockcache::EndpointInfo* out) const;
  bool FromPb(const pb::blockcache::EndpointInfo& in);
  const char* Check(uint8_t local_qps, LinkLayer local_link_layer) const;
};

// Format: [MessageHeader 24B][RemoteRegion 20B x region_count][payload]
struct __attribute__((packed)) MessageHeader {
  uint8_t type;
  uint8_t region_type;
  uint8_t region_count;
  uint8_t reserved0;
  uint16_t opcode;
  ReplyCode code;
  uint32_t payload_len;
  uint16_t credit;
  uint16_t reserved1;
  uint64_t correlation_id;
};
static_assert(sizeof(MessageHeader) == 24,
              "MessageHeader is a protocol structure");
static_assert(alignof(MessageHeader) == 1,
              "MessageHeader is read straight off the wire");
static_assert(offsetof(MessageHeader, credit) == 12,
              "MessageHeader must have no implicit padding");
static_assert(offsetof(MessageHeader, correlation_id) == 16,
              "MessageHeader must have no implicit padding");

struct HeaderParam {
  MessageType type;
  RegionType region_type = RegionType::kNone;
  Opcode opcode = kOpUnspecified;
  ReplyCode code = kReplyOk;
  uint64_t correlation_id = 0;
  uint8_t region_count = 0;
};

struct MessageBuffer {
  void* data = nullptr;
  uint32_t capacity = 0;
};

struct MessageView {
  const MessageHeader* header = nullptr;
  std::string_view payload;

  MessageType type() const { return static_cast<MessageType>(header->type); }
  Opcode opcode() const { return header->opcode; }
  ReplyCode code() const { return header->code; }
  bool accepted() const { return header->code == kReplyOk; }
  uint64_t correlation_id() const { return header->correlation_id; }
  uint16_t credit() const { return header->credit; }

  RegionType region_type() const {
    return static_cast<RegionType>(header->region_type);
  }

  std::span<const RemoteRegion> regions() const {
    return {reinterpret_cast<const RemoteRegion*>(header + 1),
            header->region_count};
  }

  uint32_t attachment_size() const {
    if (region_type() != RegionType::kToServer) {
      return 0;
    }
    return static_cast<uint32_t>(
        std::min<uint64_t>(GetLength(regions()), UINT32_MAX));
  }
};

class Protocol {
 public:
  static constexpr uint16_t kHandshakeVersion = 1;
  static constexpr uint16_t kMinRecvCredits = 2;
  static constexpr uint32_t kMaxInflightRpcs = (uint32_t{UINT16_MAX} - 2) / 2;
  static constexpr uint8_t kMaxRegions = kMaxBufferViews;

  static constexpr bool IsValidInflightRpcs(uint32_t value) {
    return value > 0 && value <= kMaxInflightRpcs;
  }

  static uint32_t MessageBudget() {
    return (2u * FLAGS_rdma_max_inflight_rpcs) + 2u;
  }
  static uint32_t MsgSendWr() { return MessageBudget(); }
  static uint32_t MsgRecvWr() { return MessageBudget(); }
  static uint32_t SendBufferCount() { return MessageBudget(); }

  static size_t MessageSize(uint8_t region_count, uint32_t payload_len) {
    return sizeof(MessageHeader) +
           (size_t{region_count} * sizeof(RemoteRegion)) + payload_len;
  }

  static void SetCredit(MessageBuffer message, uint16_t credit);
  static RemoteRegion* GetRegions(MessageBuffer message);

  static uint32_t EncodeMessage(MessageBuffer message, const HeaderParam& param,
                                const google::protobuf::Message* body);
  static const char* DecodeMessage(BufferView message, MessageView* out);
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_PROTOCOL_H_
