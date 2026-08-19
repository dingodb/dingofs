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

#include "blockcache/net/server/channel.h"

#include <glog/logging.h>

#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/net/rdma/connection.h"

namespace dingofs {
namespace blockcache {

Channel::~Channel() {
  LOG_IF(WARNING, conn_ != nullptr) << "Channel destroyed without Shutdown()";
}

Future<Status> Channel::Start(Client* bootstrap, uint64_t route_hint,
                              ChannelOption option) {
  option_ = option;
  StatusOr<std::unique_ptr<ClientDomain>> made =
      ClientDomain::Create(option_.rdma);
  if (!made.ok()) {
    co_return made.status();
  }
  owned_domain_ = std::move(made).value();

  const Status status =
      co_await Dial(owned_domain_->domain(), bootstrap, route_hint);
  if (!status.ok()) {
    co_await owned_domain_->Shutdown();
    owned_domain_.reset();
    domain_ = nullptr;
  }
  co_return status;
}

Future<Status> Channel::Dial(RdmaDomain* domain, Client* bootstrap,
                             uint64_t route_hint) {
  CHECK(domain != nullptr) << "the connection needs an domain to live in";
  CHECK(bootstrap != nullptr) << "the handshake needs a wire to ride";
  CHECK(client_ == nullptr) << "Channel already has a client";

  domain_ = domain;
  StatusOr<RdmaConnection*> conn =
      co_await domain_->Connect(*bootstrap, route_hint);
  if (!conn.ok()) {
    domain_ = nullptr;
    co_return conn.status();
  }
  conn_ = conn.value();
  client_ = conn_;
  co_return Status::OK();
}

void Channel::Adopt(std::unique_ptr<Client> client) {
  CHECK(client_ == nullptr) << "Channel already has a client";
  owned_client_ = std::move(client);
  client_ = owned_client_.get();
}

void Channel::Borrow(Client* client) {
  CHECK(client != nullptr) << "a channel needs a client";
  CHECK(client_ == nullptr) << "Channel already has a client";
  client_ = client;
}

Future<> Channel::Shutdown() {
  client_ = nullptr;
  owned_client_.reset();

  // A borrowed domain keeps serving its other channels, so only this
  // connection goes; an owned one goes whole.
  if (conn_ != nullptr && owned_domain_ == nullptr) {
    co_await domain_->Retire(conn_);
  }
  conn_ = nullptr;
  domain_ = nullptr;

  if (owned_domain_ != nullptr) {
    co_await owned_domain_->Shutdown();
    owned_domain_.reset();
  }
}

Future<StatusOr<Reply>> Channel::Send(Opcode opcode, std::string_view payload,
                                      Body body, uint64_t route_hint) {
  if (client_ == nullptr) {
    return MakeReadyFuture<StatusOr<Reply>>(
        Status::Internal("channel is not connected"));
  }
  // On rdma the shard was chosen by dialling; only brpc wires the hint.
  return client_->Call(opcode, payload, body, route_hint);
}

}  // namespace blockcache
}  // namespace dingofs
