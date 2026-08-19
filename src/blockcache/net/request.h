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

#ifndef DINGOFS_BLOCKCACHE_NET_REQUEST_H_
#define DINGOFS_BLOCKCACHE_NET_REQUEST_H_

#include <cstdint>
#include <string_view>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/body.h"
#include "blockcache/net/types.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

// One in-flight request plus the means to answer it, transport-neutral.
// INV-1: replying completes the request; payload() is empty afterwards.
// INV-2: ReplyWithBody resolves when framework is done with `body`, not peer.
class Request {
 public:
  virtual ~Request() = default;

  Request(const Request&) = delete;
  Request& operator=(const Request&) = delete;

  // Answers the caller, exactly once per request.
  virtual Future<Status> Reply(ReplyCode code, std::string_view payload) = 0;

  // Pulls the body into `dst` (>= request_body_size()); once, before replying.
  virtual Future<Status> FetchBody(BufferView dst) = 0;

  // Answers with `payload` plus `body`; see INV-2 for body lifetime.
  Future<Status> ReplyWithBody(ReplyCode code, std::string_view payload,
                               BufferView body) {
    if (body.empty()) {
      return Reply(code, payload);
    }
    if (body.size > kMaxBodyBytes) {
      return Reply(kReplyTooLarge, payload);
    }
    return DoReplyWithBody(code, payload, body);
  }

  Opcode opcode() const { return opcode_; }
  // Points into the arrival buffer; valid until reply (INV-1) or return.
  std::string_view payload() const { return payload_; }
  // Body bytes the caller offers (shape kToServer); zero otherwise.
  uint32_t request_body_size() const {
    return shape_ == BodyShape::kToServer ? body_bytes_ : 0;
  }
  bool replied() const { return replied_; }
  // False once the peer is gone; lets the dispatcher skip the error reply.
  virtual bool alive() const { return true; }

 protected:
  // Transport's half: `body` is non-empty and within kMaxBodyBytes.
  virtual Future<Status> DoReplyWithBody(ReplyCode code,
                                         std::string_view payload,
                                         BufferView body) = 0;

  Request(Opcode opcode, std::string_view payload, BodyShape shape,
          uint32_t body_bytes)
      : payload_(payload),
        body_bytes_(body_bytes),
        opcode_(opcode),
        shape_(shape) {}

  // Every Reply override calls this on completion; clears payload_ (INV-1).
  void MarkReplied() {
    replied_ = true;
    payload_ = {};
  }

  std::string_view payload_;
  uint32_t body_bytes_ = 0;
  Opcode opcode_;
  BodyShape shape_ = BodyShape::kNone;
  bool replied_ = false;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_REQUEST_H_
