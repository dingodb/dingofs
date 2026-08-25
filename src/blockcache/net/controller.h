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

#ifndef DINGOFS_BLOCKCACHE_NET_CONTROLLER_H_
#define DINGOFS_BLOCKCACHE_NET_CONTROLLER_H_

#include <glog/logging.h>

#include <string>
#include <utility>

#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/memory/buffer_view.h"

namespace dingofs {
namespace blockcache {

class Controller {
 public:
  Controller() = default;

  Controller(const Controller&) = delete;
  Controller& operator=(const Controller&) = delete;

  void SetFailed(int error_code, std::string reason) {
    error_code_ = error_code == 0 ? -1 : error_code;
    if (error_text_.empty()) {
      error_text_ = std::move(reason);
    } else {
      error_text_.push_back(' ');
      error_text_.append(reason);
    }
  }
  void SetFailed(std::string reason) { SetFailed(-1, std::move(reason)); }
  bool Failed() const { return error_code_ != 0; }
  int ErrorCode() const { return error_code_; }
  const std::string& ErrorText() const { return error_text_; }

  Buffer& request_attachment() { return request_attachment_; }
  const Buffer& request_attachment() const { return request_attachment_; }

  Buffer& response_attachment() { return response_attachment_; }
  const Buffer& response_attachment() const { return response_attachment_; }

  // The views and their backing buffers must remain valid until the RPC
  // finishes. The controller borrows them without taking ownership.
  void set_borrowed_request_attachment(BufferViews attachment) {
    borrowed_request_attachment_ = attachment;
  }
  void set_borrowed_response_attachment(BufferView attachment) {
    borrowed_response_attachment_ = attachment;
  }

  BufferViews request_attachment_views() {
    DCHECK(request_attachment_.empty() || borrowed_request_attachment_.empty())
        << "request attachment cannot be both owned and borrowed";
    if (request_attachment_.empty()) {
      return borrowed_request_attachment_;
    }
    request_attachment_view_ = request_attachment_.view();
    return {&request_attachment_view_, 1};
  }
  BufferView response_attachment_view() const {
    DCHECK(response_attachment_.empty() ||
           borrowed_response_attachment_.empty())
        << "response attachment cannot be both owned and borrowed";
    return response_attachment_.empty() ? borrowed_response_attachment_
                                        : response_attachment_.view();
  }

 private:
  int error_code_ = 0;
  std::string error_text_;

  Buffer request_attachment_;
  Buffer response_attachment_;
  BufferViews borrowed_request_attachment_;
  BufferView request_attachment_view_;
  BufferView borrowed_response_attachment_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif
