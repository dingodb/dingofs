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

#ifndef DINGOFS_CACHE_V2_CORE_NET_BRPC_REQUEST_H_
#define DINGOFS_CACHE_V2_CORE_NET_BRPC_REQUEST_H_

#include <string_view>

#include "cache/v2/core/net/request.h"

namespace dingofs {
namespace cache {
namespace v2 {

struct ServerCall;

// What a handler sees when the request arrived over brpc.
class BrpcRequest final : public Request {
 public:
  BrpcRequest(ServerCall* call, Opcode opcode, std::string_view payload,
              BodyShape shape, uint32_t body_bytes)
      : Request(opcode, payload, shape, body_bytes), call_(call) {}

  // Replying completes the request; everything brpc owns is gone afterwards.
  Future<Status> Reply(ReplyCode code, std::string_view payload) override;

  // FetchBody costs one memcpy (brpc's blocks); ReplyWithBody costs none.
  Future<Status> FetchBody(BufferView dst) override;

 protected:
  Future<Status> DoReplyWithBody(ReplyCode code, std::string_view payload,
                                 BufferView body) override;

 private:
  ServerCall* call_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_BRPC_REQUEST_H_
