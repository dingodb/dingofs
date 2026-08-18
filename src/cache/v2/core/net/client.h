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

#ifndef DINGOFS_CACHE_V2_CORE_NET_CLIENT_H_
#define DINGOFS_CACHE_V2_CORE_NET_CLIENT_H_

#include <cstdint>
#include <string_view>
#include <utility>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/body.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/reactor/coroutine.h"

namespace dingofs {
namespace cache {
namespace v2 {

// A reply; code() is the HANDLER's verdict, not the call's Status.
// Payload lives in the transport's receive buffer until this dies; copy it.
class Reply {
 public:
  using ReleaseFn = void (*)(void* owner, uint64_t token);

  Reply() = default;
  Reply(ReplyCode code, std::string_view payload, ReleaseFn release = nullptr,
        void* owner = nullptr, uint64_t token = 0)
      : payload_(payload),
        release_(release),
        owner_(owner),
        token_(token),
        code_(code) {}

  ~Reply() { Release(); }

  Reply(const Reply&) = delete;
  Reply& operator=(const Reply&) = delete;

  Reply(Reply&& o) noexcept
      : payload_(o.payload_),
        release_(std::exchange(o.release_, nullptr)),
        owner_(o.owner_),
        token_(o.token_),
        code_(o.code_) {}

  Reply& operator=(Reply&& o) noexcept {
    if (this != &o) {
      Release();
      payload_ = o.payload_;
      release_ = std::exchange(o.release_, nullptr);
      owner_ = o.owner_;
      token_ = o.token_;
      code_ = o.code_;
    }
    return *this;
  }

  ReplyCode code() const { return code_; }
  bool accepted() const { return code_ == kReplyOk; }  // the handler said yes
  std::string_view payload() const { return payload_; }

 private:
  void Release() {
    if (release_ != nullptr) {
      release_(owner_, token_);
      release_ = nullptr;
    }
  }

  std::string_view payload_;
  ReleaseFn release_ = nullptr;
  void* owner_ = nullptr;
  uint64_t token_ = 0;
  ReplyCode code_ = 0;
};

// The caller's side of a transport: verb + payload + optional body -> reply.
// Buffers must stay valid until the returned future resolves.
// `route_hint`: caller's key hash, mapped by ShardOf on the server; 0 = any.
class Client {
 public:
  virtual ~Client() = default;

  virtual Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload,
                                       Body body, uint64_t route_hint) = 0;

  // Non-virtual: default args on virtuals bind statically.
  Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload,
                               Body body) {
    return Call(opcode, payload, body, 0);
  }
  Future<StatusOr<Reply>> Call(Opcode opcode, std::string_view payload) {
    return Call(opcode, payload, Body::None(), 0);
  }
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_CLIENT_H_
