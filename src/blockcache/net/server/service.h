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

#ifndef DINGOFS_BLOCKCACHE_NET_SERVER_SERVICE_H_
#define DINGOFS_BLOCKCACHE_NET_SERVER_SERVICE_H_

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/protocol/codec.h"
#include "blockcache/net/request.h"
#include "blockcache/net/server/controller.h"
#include "blockcache/net/types.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

enum class MethodFlags : uint32_t {
  kNone = 0,
  // No body pre-fetch; the method calls FetchRequestAttachment itself.
  kManualBody = 1u << 0,
};

inline bool HasFlag(MethodFlags v, MethodFlags f) {
  return (static_cast<uint32_t>(v) & static_cast<uint32_t>(f)) != 0;
}

// Service base; one instance per shard, methods are coroutines (no `done`).
class Service {
 public:
  using Method = std::function<Future<Status>(Request&)>;

  Service() = default;
  virtual ~Service() = default;

  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  virtual void VerifyBound() const {}

  bool Has(Opcode opcode) const {
    return opcode < methods_.size() && methods_[opcode] != nullptr;
  }
  const Method& MethodOf(Opcode opcode) const { return methods_[opcode]; }
  size_t method_limit() const { return methods_.size(); }
  unsigned shard() const { return shard_; }
  void set_shard(unsigned shard) { shard_ = shard; }

 protected:
  template <typename S, typename Req, typename Resp>
  void AddMethod(Opcode opcode,
                 Future<> (S::*method)(Controller*, const Req*, Resp*),
                 MethodFlags flags = MethodFlags::kNone) {
    auto* self = static_cast<S*>(this);
    Bind(opcode, [self, method, flags](Request& request) {
      return Trampoline<S, Req, Resp>(self, method, flags, request);
    });
  }

 private:
  void Bind(Opcode opcode, Method method) {
    if (methods_.size() <= opcode) {
      methods_.resize(size_t{opcode} + 1);
    }
    methods_[opcode] = std::move(method);
  }

  // Decode -> (fetch body) -> method -> encode -> reply.
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
  template <typename S, typename Req, typename Resp, typename M>
  static Future<Status> Trampoline(S* self, M method, MethodFlags flags,
                                   Request& request) {
    Controller cntl;
    cntl.BindRequest(&request);

    Req req;
    if (!Codec<Req>::Decode(request.payload(), &req)) {
      co_return co_await request.Reply(kReplyBadRequest, {});
    }

    const uint32_t body_size = request.request_body_size();
    if (!HasFlag(flags, MethodFlags::kManualBody) && body_size > 0) {
      // Slab-backed: 4 KiB-aligned address, so O_DIRECT can land straight in
      // it (SlabPool::kMinShift). The length stays the caller's logical size.
      Buffer body = Buffer::Alloc(body_size);
      if (body.empty()) {
        co_return co_await request.Reply(kReplyTooLarge, {});
      }
      const Status status = co_await request.FetchBody(body.view());
      if (!status.ok()) {
        co_return status;
      }
      cntl.request_attachment() = std::move(body);
    }

    Resp resp;
    // A method answers in `resp`; a call it could not serve at all says so on
    // the controller.
    co_await (self->*method)(&cntl, &req, &resp);
    if (cntl.Failed()) {
      co_return co_await request.Reply(kReplyHandlerError, {});
    }

    const Encoded<Resp> payload(resp);
    if (!cntl.has_response_body()) {
      co_return co_await request.Reply(kReplyOk, payload.view());
    }
    // A borrowed body stays alive: the method's frame is suspended here.
    co_return co_await request.ReplyWithBody(kReplyOk, payload.view(),
                                             cntl.response_view());
  }

  std::vector<Method> methods_;
  unsigned shard_ = 0;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_SERVER_SERVICE_H_
