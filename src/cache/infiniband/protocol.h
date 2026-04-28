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

/*
 * Project: DingoFS
 * Created Date: 2026-05-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_

#include <google/protobuf/message.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

// Wire format (per frame):
//   +-----------------------------------------------------+
//   | Header (24 B, fixed)                                |
//   |   uint32 magic           = 'RDMA' (0x52444D41)      |
//   |   uint32 meta_len        bytes of metadata section  |
//   |   uint32 data_len        bytes of data section      |
//   |   uint64 correlation_id                             |
//   +-----------------------------------------------------+
//   | Metadata (meta_len bytes)                           |
//   |   RequestMeta or ResponseMeta protobuf              |
//   +-----------------------------------------------------+
//   | Data (data_len bytes)                               |
//   |   business Request/Response protobuf serialized raw |
//   +-----------------------------------------------------+
class Protocol {
 public:
  struct MessageHeader {
    uint32_t magic;
    uint32_t meta_len;
    uint32_t data_len;
    uint64_t correlation_id;
  };

  static constexpr size_t kHeaderSize = 24;
  static constexpr uint32_t kMagic = 0x52444D41;  // 'R' 'D' 'M' 'A'
  static_assert(sizeof(MessageHeader) == kHeaderSize,
                "MessageHeader size must match Protocol::kHeaderSize");

  static Status SerializeRequest(
      uint64_t correlation_id, const pb::infiniband::RequestMeta& request_meta,
      const google::protobuf::Message& request, RdmaBuffer* buffer);
  static Status SerializeResponse(
      uint64_t correlation_id,
      const pb::infiniband::ResponseMeta& response_meta,
      const google::protobuf::Message* response, RdmaBuffer* buffer);

  static Status PeekCorrelationId(const RdmaBuffer* buffer,
                                  uint64_t* correlation_id);
  static Status ParseRequest(const RdmaBuffer* buffer, uint64_t* correlation_id,
                             pb::infiniband::RequestMeta* request_meta,
                             std::string_view* request_view);
  static Status ParseResponse(const RdmaBuffer* buffer,
                              uint64_t* correlation_id,
                              pb::infiniband::ResponseMeta* response_meta,
                              std::string_view* response_view);

 private:
  static Status Serialize(uint64_t correlation_id,
                          const google::protobuf::Message& meta,
                          const google::protobuf::Message* data,
                          RdmaBuffer* buffer);
  static Status ParseHeader(const RdmaBuffer* buffer,
                            MessageHeader** header_out);
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_
