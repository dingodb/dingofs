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
 * Created Date: 2026-05-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_

#include <bthread/countdown_event.h>
#include <butil/iobuf.h>
#include <gflags/gflags_declare.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Protocol;
class Messenger;
class ClientSession;
class ServerSession;

class Controller : public ::google::protobuf::RpcController {
 public:
  Controller() = default;
  ~Controller() override = default;

  void Reset() override {
    failed_ = false;
    error_text_.clear();
    error_code_ = pb::infiniband::ErrorCode::Ok;
    response_meta_.Clear();
  }

  bool Failed() const override { return failed_; }

  int32_t ErrorCode() const { return error_code_; }
  std::string ErrorText() const override { return error_text_; }

  void StartCancel() override {}

  void SetErrorCode(int32_t error_code) { error_code_ = error_code; }

  void SetFailed(const std::string& reason) override {
    failed_ = true;
    error_text_ = reason;
  }

  bool IsCanceled() const override { return false; }

  void NotifyOnCancel(::google::protobuf::Closure* /*callback*/) override {}

  // RpcController interface end

  int32_t& error_code() { return error_code_; }

  uint64_t& correlation_id() { return correlation_id_; }
  const uint64_t& correlation_id() const { return correlation_id_; }

  const IOBuffer& request_attachment() const { return request_attachment_; }
  IOBuffer& request_attachment() { return request_attachment_; }

  const IOBuffer& response_attachment() const { return response_attachment_; }
  IOBuffer& response_attachment() { return response_attachment_; }

  void SetRequestRdmaRegion(uint64_t addr, uint32_t length, uint32_t rkey) {
    request_meta_.set_addr(addr);
    request_meta_.set_length(length);
    request_meta_.set_rkey(rkey);
  }

  void SetRequestRdmaRegion(const void* addr, uint32_t length, uint32_t rkey) {
    SetRequestRdmaRegion(reinterpret_cast<uint64_t>(addr), length, rkey);
  }

  void SetRequestAttachmentRegion(uint64_t addr, uint32_t length,
                                  uint32_t rkey) {
    request_meta_.set_attachment_size(length);
    SetRequestRdmaRegion(addr, length, rkey);
  }

  void SetRequestAttachmentRegion(const void* addr, uint32_t length,
                                  uint32_t rkey) {
    SetRequestAttachmentRegion(reinterpret_cast<uint64_t>(addr), length, rkey);
  }

  bthread::CountdownEvent& request_sent() { return request_sent_; }
  bthread::CountdownEvent& response_sent() { return response_sent_; }
  bthread::CountdownEvent& response_received() { return response_received_; }
  bthread::CountdownEvent& request_attachment_read() {
    return request_attachment_read_;
  }

  size_t request_attachment_size() { return request_meta_.attachment_size(); }

  pb::infiniband::RequestMeta& request_meta() { return request_meta_; }
  pb::infiniband::ResponseMeta& response_meta() { return response_meta_; }
  google::protobuf::Service*& service() { return servie_; }
  google::protobuf::MethodDescriptor*& method() { return method_; };
  google::protobuf::Message*& request() { return request_; }
  google::protobuf::Message*& response() { return response_; }
  bool CanRespond() const { return correlation_id_ != 0; }
  google::protobuf::Arena* Arena() { return &arena_; }

 private:
  friend class Protocol;
  friend class Messenger;
  friend class ClientSession;
  friend class ServerSession;

  // header
  uint64_t correlation_id_{0};
  uint32_t meta_len_{0};
  uint32_t data_len_{0};
  std::string_view data_view_;

  // meta
  pb::infiniband::RequestMeta request_meta_;
  pb::infiniband::ResponseMeta response_meta_;

  // data
  google::protobuf::Service* servie_{nullptr};
  google::protobuf::MethodDescriptor* method_{nullptr};
  google::protobuf::Message* request_{nullptr};
  google::protobuf::Message* response_{nullptr};

  IOBuffer request_attachment_;
  IOBuffer response_attachment_;

  google::protobuf::Arena arena_;

  // Raw bytes of the in-band request/response body inside the recv buffer;
  // set by Protocol::Parse{Request,Response}, consumed by Messenger::Dispatch.

  bthread::CountdownEvent request_sent_{1};
  bthread::CountdownEvent request_attachment_read_{1};
  bthread::CountdownEvent response_sent_{1};
  bthread::CountdownEvent response_received_{1};

  int32_t error_code_{pb::infiniband::ErrorCode::Ok};
  bool failed_{false};
  std::string error_text_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
