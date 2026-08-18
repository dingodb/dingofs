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

#ifndef DINGOFS_CACHE_V2_CORE_SERVER_CHANNEL_H_
#define DINGOFS_CACHE_V2_CORE_SERVER_CHANNEL_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/client.h"
#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/option.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/core/server/client_domain.h"
#include "cache/v2/core/server/codec.h"
#include "cache/v2/core/server/controller.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

struct ChannelOption {
  RdmaOption rdma;
};

// Calling side, shaped like brpc::Channel; belongs to its creating shard.
class Channel {
 public:
  Channel() = default;
  ~Channel();

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  // Dials rdma over an domain of its own; the handshake rides `bootstrap`
  // (borrowed for this call). Prefer Dial() when the shard has a
  // ClientDomain: one domain per channel does not scale past a handful.
  Future<Status> Start(Client* bootstrap, uint64_t route_hint,
                       ChannelOption option);

  // Same handshake over an domain someone else owns. Shutdown then retires
  // this channel's connection and leaves the domain running.
  Future<Status> Dial(RdmaDomain* domain, Client* bootstrap,
                      uint64_t route_hint);

  // Adopts a caller-built client (the only way brpc arrives).
  void Adopt(std::unique_ptr<Client> client);

  // Same, for a client the caller keeps and outlives this channel with --
  // one brpc socket can back several channels.
  void Borrow(Client* client);

  Future<> Shutdown();

  template <typename Req, typename Resp>
  Future<Status> Call(Opcode opcode, const Req* request, Resp* response,
                      Controller* cntl) {
    const Encoded<Req> payload(*request);

    // A call carries its body one way, not both. Owned or borrowed reads the
    // same here; only who frees the memory differs.
    const BufferViews to_server = cntl->request_ranges();
    const BufferView to_client = cntl->response_view();
    if (!to_server.empty() && !to_client.empty()) {
      cntl->SetFailed(EINVAL, "both attachments are set on one call");
      co_return Status::Internal("a call carries a body one way, not both");
    }
    Body body = Body::None();
    if (!to_server.empty()) {
      body = Body::Send(to_server);
    } else if (!to_client.empty()) {
      body = Body::Recv(to_client);
    }

    StatusOr<Reply> reply =
        co_await Send(opcode, payload.view(), body, cntl->route_hint());
    if (!reply.ok()) {
      cntl->SetFailed(EIO, reply.status().ToString());
      co_return reply.status();
    }

    cntl->set_reply_code(reply.value().code());
    if (!reply.value().accepted()) {
      cntl->SetFailed(EIO, "the peer's handler rejected the request");
      co_return Status::Internal("rpc rejected: code=" +
                                 std::to_string(reply.value().code()));
    }
    if (!Codec<Resp>::Decode(reply.value().payload(), response)) {
      cntl->SetFailed(EIO, "the reply payload does not decode");
      co_return Status::Internal("fail to decode the rpc reply");
    }
    co_return Status::OK();
  }

  bool connected() const { return client_ != nullptr; }
  // The queue pair behind this channel; null on a brpc one.
  RdmaConnection* connection() const { return conn_; }

 private:
  Future<StatusOr<Reply>> Send(Opcode opcode, std::string_view payload,
                               Body body, uint64_t route_hint);

  ChannelOption option_;
  // Set only when this channel dialled over an domain of its own.
  std::unique_ptr<ClientDomain> owned_domain_;
  // Whoever's domain the connection lives in, and the connection itself.
  RdmaDomain* domain_ = nullptr;
  RdmaConnection* conn_ = nullptr;
  // Set when adopted; otherwise client_ is conn_.
  std::unique_ptr<Client> owned_client_;
  Client* client_ = nullptr;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_SERVER_CHANNEL_H_
