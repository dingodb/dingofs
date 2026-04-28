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

#include "cache/infiniband/protocol.h"

#include <glog/logging.h>

#include <cstdint>
#include <string_view>

namespace dingofs {
namespace cache {
namespace infiniband {

Status Protocol::Serialize(uint64_t correlation_id,
                           const google::protobuf::Message& meta,
                           const google::protobuf::Message* data,
                           RdmaBuffer* buffer) {
  CHECK_NOTNULL(buffer);
  const size_t meta_len = meta.ByteSizeLong();
  const size_t data_len = (data != nullptr) ? data->ByteSizeLong() : 0;
  const size_t total = kHeaderSize + meta_len + data_len;
  if (buffer->capacity < total) {
    LOG(ERROR) << "RDMA buffer too small: need=" << total
               << " capacity=" << buffer->capacity;
    return Status::InvalidParam("rdma buffer too small");
  }

  // header
  auto* header = reinterpret_cast<MessageHeader*>(buffer->data);
  header->magic = kMagic;
  header->meta_len = static_cast<uint32_t>(meta_len);
  header->data_len = static_cast<uint32_t>(data_len);
  header->correlation_id = correlation_id;

  // meta
  char* ptr = buffer->data + kHeaderSize;
  if (!meta.SerializeToArray(ptr, static_cast<int>(meta_len))) {
    return Status::Internal("serialize meta failed");
  }

  // data
  if (data_len > 0 &&
      !data->SerializeToArray(ptr + meta_len, static_cast<int>(data_len))) {
    return Status::Internal("serialize data failed");
  }

  buffer->length = static_cast<uint32_t>(total);
  return Status::OK();
}

Status Protocol::SerializeRequest(
    uint64_t correlation_id, const pb::infiniband::RequestMeta& request_meta,
    const google::protobuf::Message& request, RdmaBuffer* buffer) {
  return Serialize(correlation_id, request_meta, &request, buffer);
}

Status Protocol::SerializeResponse(
    uint64_t correlation_id, const pb::infiniband::ResponseMeta& response_meta,
    const google::protobuf::Message* response, RdmaBuffer* buffer) {
  return Serialize(correlation_id, response_meta, response, buffer);
}

Status Protocol::ParseHeader(const RdmaBuffer* buffer,
                             MessageHeader** header_out) {
  CHECK_NOTNULL(buffer);
  if (buffer->length < kHeaderSize) {
    return Status::InvalidParam("rdma buffer smaller than header");
  }

  auto* header = reinterpret_cast<MessageHeader*>(buffer->data);
  if (header->magic != kMagic) {
    LOG(ERROR) << "Invalid magic: " << std::hex << header->magic;
    return Status::InvalidParam("invalid magic");
  }

  const size_t need = kHeaderSize + static_cast<size_t>(header->meta_len) +
                      static_cast<size_t>(header->data_len);
  if (buffer->length < need) {
    LOG(WARNING) << "Incomplete frame: need=" << need
                 << " available=" << buffer->length;
    return Status::InvalidParam("incomplete frame");
  }

  *header_out = header;
  return Status::OK();
}

Status Protocol::PeekCorrelationId(const RdmaBuffer* buffer,
                                   uint64_t* correlation_id) {
  MessageHeader* header = nullptr;
  auto status = ParseHeader(buffer, &header);
  if (!status.ok()) {
    return status;
  }

  *correlation_id = header->correlation_id;
  return Status::OK();
}

Status Protocol::ParseRequest(const RdmaBuffer* buffer,
                              uint64_t* correlation_id,
                              pb::infiniband::RequestMeta* request_meta,
                              std::string_view* request_view) {
  MessageHeader* header = nullptr;
  auto status = ParseHeader(buffer, &header);
  if (!status.ok()) {
    return status;
  }

  const char* ptr = buffer->data + kHeaderSize;
  if (!request_meta->ParseFromArray(ptr, static_cast<int>(header->meta_len))) {
    return Status::InvalidParam("parse request meta failed");
  }

  *correlation_id = header->correlation_id;
  *request_view = std::string_view(ptr + header->meta_len, header->data_len);
  return Status::OK();
}

Status Protocol::ParseResponse(const RdmaBuffer* buffer,
                               uint64_t* correlation_id,
                               pb::infiniband::ResponseMeta* response_meta,
                               std::string_view* response_view) {
  MessageHeader* header = nullptr;
  auto status = ParseHeader(buffer, &header);
  if (!status.ok()) {
    return status;
  }

  const char* ptr = buffer->data + kHeaderSize;
  if (!response_meta->ParseFromArray(ptr, static_cast<int>(header->meta_len))) {
    return Status::InvalidParam("parse response meta failed");
  }

  *correlation_id = header->correlation_id;
  *response_view = std::string_view(ptr + header->meta_len, header->data_len);
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
