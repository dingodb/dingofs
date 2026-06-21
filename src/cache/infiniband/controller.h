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

#include <google/protobuf/arena.h>
#include <google/protobuf/service.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/io_buffer.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Controller : public ::google::protobuf::RpcController {
 public:
  Controller() = default;
  ~Controller() override = default;

  void Reset() override {
    failed_ = false;
    error_text_.clear();
    error_code_ = pb::infiniband::ErrorCode::Ok;
    response_attachment_size_ = 0;
    timeout_ms_ = -1;
  }

  int32_t ErrorCode() const { return error_code_; }
  void SetErrorCode(int32_t error_code) { error_code_ = error_code; }
  int64_t timeout_ms() const { return timeout_ms_; }
  void set_timeout_ms(int64_t timeout_ms) { timeout_ms_ = timeout_ms; }

  bool Failed() const override { return failed_; }
  std::string ErrorText() const override { return error_text_; }
  void SetFailed(const std::string& reason) override {
    failed_ = true;
    error_text_ = reason;
  }

  void StartCancel() override {}
  bool IsCanceled() const override { return false; }
  void NotifyOnCancel(::google::protobuf::Closure* /*callback*/) override {}

  google::protobuf::Arena* Arena() { return &arena_; }

  pb::infiniband::RDMARegion& write_region() { return write_region_; }
  google::protobuf::RepeatedPtrField<pb::infiniband::RDMARegion>&
  read_regions() {
    return read_regions_;
  };

  size_t response_attachment_size() const { return response_attachment_size_; }
  void set_response_attachment_size(size_t size) {
    response_attachment_size_ = size;
  }

  const IOBuffer& request_attachment() const { return request_attachment_; }
  IOBuffer& request_attachment() { return request_attachment_; }

  const IOBuffer& response_attachment() const { return response_attachment_; }
  IOBuffer& response_attachment() { return response_attachment_; }

 private:
  int32_t error_code_{pb::infiniband::ErrorCode::Ok};
  bool failed_{false};
  std::string error_text_;

  google::protobuf::Arena arena_;
  pb::infiniband::RDMARegion write_region_;
  google::protobuf::RepeatedPtrField<pb::infiniband::RDMARegion> read_regions_;
  size_t response_attachment_size_{0};
  int64_t timeout_ms_{-1};
  IOBuffer request_attachment_;
  IOBuffer response_attachment_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
