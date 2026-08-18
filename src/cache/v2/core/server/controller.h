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

#ifndef DINGOFS_CACHE_V2_CORE_SERVER_CONTROLLER_H_
#define DINGOFS_CACHE_V2_CORE_SERVER_CONTROLLER_H_

#include <cstdint>
#include <string>
#include <utility>

#include "cache/v2/core/memory/buffer.h"
#include "cache/v2/core/net/request.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// Per-call side channel, shaped like brpc::Controller; one type both sides.
class Controller {
 public:
  Controller() = default;

  Controller(const Controller&) = delete;
  Controller& operator=(const Controller&) = delete;

  Buffer& request_attachment() { return request_body_; }
  const Buffer& request_attachment() const { return request_body_; }
  Buffer& response_attachment() { return response_body_; }
  const Buffer& response_attachment() const { return response_body_; }

  // Borrowed memory must be registered and live until the future resolves.
  // A caller reading into a buffer it already owns borrows it here rather
  // than allocating a second one to copy out of.
  //
  // The request side takes a LIST: a writer accumulates into fixed-size
  // pages, and gathering them into one buffer first would be a whole-body
  // memcpy on every write. The array of views must outlive the call too.
  // The response side is one range -- a reader always has somewhere
  // contiguous to land.
  void set_borrowed_request(BufferViews borrowed) {
    borrowed_request_ = borrowed;
  }
  void set_borrowed_response(BufferView borrowed) {
    borrowed_response_ = borrowed;
  }
  // Non-const: an owned attachment is published as a one-element list, and
  // that element lives here.
  BufferViews request_ranges() {
    if (request_body_.empty()) {
      return borrowed_request_;
    }
    owned_request_ = request_body_.view();
    return {&owned_request_, 1};
  }
  BufferView response_view() const {
    return response_body_.empty() ? borrowed_response_ : response_body_.view();
  }
  bool has_response_body() const { return !response_view().empty(); }

  // Attachment bytes the caller advertised (0 when none).
  uint32_t request_attachment_size() const {
    return request_ == nullptr ? 0 : request_->request_body_size();
  }

  // Pulls the request body into `into` (kManualBody methods only).
  Future<Status> FetchRequestAttachment(Buffer& into) {
    if (request_ == nullptr) {
      return MakeReadyFuture<Status>(
          Status::Internal("no request is bound to this controller"));
    }
    return request_->FetchBody(into.view());
  }

  void SetFailed(int error_code, std::string reason) {
    error_code_ = error_code;
    error_text_ = std::move(reason);
  }
  void SetFailed(std::string reason) { SetFailed(EINVAL, std::move(reason)); }
  bool Failed() const { return error_code_ != 0; }
  int ErrorCode() const { return error_code_; }
  const std::string& ErrorText() const { return error_text_; }

  // Key hash that picks the serving shard via ShardOf; 0 means "no opinion".
  void set_route_hint(uint64_t key_hash) { route_hint_ = key_hash; }
  uint64_t route_hint() const { return route_hint_; }

  ReplyCode reply_code() const { return reply_code_; }
  void set_reply_code(ReplyCode code) { reply_code_ = code; }

  // Framework use only.
  void BindRequest(Request* request) { request_ = request; }

 private:
  Request* request_ = nullptr;  // bound by the framework, not owned

  Buffer request_body_;
  Buffer response_body_;
  BufferViews borrowed_request_;
  BufferView owned_request_;  // backs the one-element list above
  BufferView borrowed_response_;

  std::string error_text_;
  int error_code_ = 0;
  uint64_t route_hint_ = 0;
  ReplyCode reply_code_ = kReplyOk;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_SERVER_CONTROLLER_H_
