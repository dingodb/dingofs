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

#include "cache/infiniband/common.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

using pb::infiniband::ErrorCode;

Status Protocol::Serialize(uint64_t correlation_id,
                           const google::protobuf::Message& meta,
                           const google::protobuf::Message* data,
                           RDMABuffer* buffer) {
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
    const google::protobuf::Message& request, RDMABuffer* buffer) {
  return Serialize(correlation_id, request_meta, &request, buffer);
}

Status Protocol::SerializeResponse(
    uint64_t correlation_id, const pb::infiniband::ResponseMeta& response_meta,
    const google::protobuf::Message* response, RDMABuffer* buffer) {
  return Serialize(correlation_id, response_meta, response, buffer);
}

Status Protocol::ParseHeader(const RDMABuffer* buffer,
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

Status Protocol::PeekCorrelationId(const RDMABuffer* buffer,
                                   uint64_t* correlation_id) {
  MessageHeader* header = nullptr;
  auto status = ParseHeader(buffer, &header);
  if (!status.ok()) {
    return status;
  }

  *correlation_id = header->correlation_id;
  return Status::OK();
}

Status Protocol::ParseRequest(const RDMABuffer* buffer,
                              uint64_t* correlation_id,
                              pb::infiniband::RequestMeta* request_meta,
                              std::string_view* request_view) {
  MessageHeader* header = nullptr;
  auto status = ParseHeader(buffer, &header);
  if (!status.ok()) {
    return status;
  }
  *correlation_id = header->correlation_id;

  const char* ptr = buffer->data + kHeaderSize;
  if (!request_meta->ParseFromArray(ptr, static_cast<int>(header->meta_len))) {
    return Status::InvalidParam("parse request meta failed");
  }

  *request_view = std::string_view(ptr + header->meta_len, header->data_len);
  return Status::OK();
}

Status Protocol::ParseResponse(const RDMABuffer* buffer,
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

Status RequestSerializer::Serialize(const Context& ctx, RDMABuffer* buffer) {
  pb::infiniband::RequestMeta meta;
  *meta.mutable_service_name() = ctx.service_name;
  *meta.mutable_method_name() = ctx.method_name;
  meta.set_attachment_size(ctx.attachment_size);
  ToPbRegions(ctx.read_regions, meta.mutable_read_regions());
  ToPbRegion(ctx.write_region, meta.mutable_write_region());

  return Protocol::SerializeRequest(ctx.correlation_id, meta, *ctx.request,
                                    buffer);
}

RequestParser::RequestParser(ServiceHub* service_hub)
    : service_hub_(service_hub) {}

Status RequestParser::Parse(RDMABuffer* buffer, Result* result, Option option) {
  std::string_view request_view;
  auto status = Protocol::ParseRequest(buffer, &result->correlation_id,
                                       &result->request_meta, &request_view);
  if (!status.ok()) {
    return status;
  }

  google::protobuf::Service* service;
  status =
      service_hub_->GetService(result->request_meta.service_name(), &service);
  if (!status.ok()) {
    return status;
  }

  google::protobuf::MethodDescriptor* method;
  status = service_hub_->GetMethod(service, result->request_meta.method_name(),
                                   &method);
  if (!status.ok()) {
    return status;
  }

  CHECK_NOTNULL(option.arena);
  auto* request = service->GetRequestPrototype(method).New(option.arena);
  auto* response = service->GetResponsePrototype(method).New(option.arena);
  if (!request->ParseFromArray(request_view.data(), request_view.size())) {
    return Status::Internal("parse request failed");
  }

  result->service = service;
  result->method = method;
  result->request = request;
  result->response = response;
  return Status::OK();
}

Status ResponseSerializer::Serialize(const Context& ctx, RDMABuffer* buffer) {
  pb::infiniband::ResponseMeta meta;
  meta.set_error_code(ctx.error_code);
  meta.set_error_message(ctx.error_message);
  meta.set_attachment_size(ctx.attachment_size);
  auto status = Protocol::SerializeResponse(ctx.correlation_id, meta,
                                            ctx.response, buffer);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status ResponseParser::Parse(RDMABuffer* buffer, Result* result,
                             google::protobuf::Message* response) {
  pb::infiniband::ResponseMeta response_meta;
  std::string_view response_view;
  auto status = Protocol::ParseResponse(buffer, &result->correlation_id,
                                        &response_meta, &response_view);
  if (!status.ok()) {
    return status;
  }

  result->error_code = response_meta.error_code();
  result->error_message = response_meta.error_message();
  result->attachment_size = response_meta.attachment_size();
  if (result->error_code == ErrorCode::Ok) {
    if (!response->ParseFromArray(response_view.data(), response_view.size())) {
      return Status::Internal("parse response failed");
    }
  }

  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
