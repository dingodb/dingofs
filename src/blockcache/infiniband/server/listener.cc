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

#include "blockcache/infiniband/server/listener.h"

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/server/session.h"
#include "blockcache/infiniband/server/session_manager.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Listener::Listener(Device* device, BufferPool* buffer_pool,
                   CompletionQueue* completion_queue)
    : device_(device),
      buffer_pool_(buffer_pool),
      completion_queue_(completion_queue) {}

StatusOr<ConnectionUPtr> Listener::Accept(const HandshakeMsg& peer,
                                          HandshakeMsg* local) {
  Status status = CheckPeerInfo(peer);
  if (!status.ok()) {
    return status;
  }

  StatusOr<QueuePairGroup> qps =
      QueuePairGroup::Create(device_, completion_queue_);
  if (!qps.ok()) {
    return qps.status();
  }

  GetLocalInfo(qps.value(), local);

  status = qps.value().ModifyToReady({peer.qp_infos, peer.num_qps});
  if (!status.ok()) {
    return status;
  }

  return std::make_unique<Connection>(
      std::move(qps).value(), buffer_pool_,
      ConnectionOption{.max_send_credits = peer.rpc_credits});
}

Status Listener::CheckPeerInfo(const HandshakeMsg& peer) const {
  auto qp_count = static_cast<uint8_t>(1 + FLAGS_rdma_bulk_qps);
  const char* reason = peer.Check(qp_count, device_->port_info().link_layer);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to handshake with peer: " << reason;
    return ToStatus(EPROTO, reason);
  }
  return Status::OK();
}

void Listener::GetLocalInfo(const QueuePairGroup& qps,
                            HandshakeMsg* msg) const {
  const PortInfo& port = device_->port_info();

  std::memset(msg, 0, sizeof(*msg));
  msg->version = Protocol::kHandshakeVersion;
  msg->shard = static_cast<uint16_t>(HasReactor() ? ThisShardId() : 0);
  msg->num_qps = qps.qp_count();
  msg->link_layer = static_cast<uint8_t>(port.link_layer);
  msg->rpc_credits = static_cast<uint16_t>(Protocol::MsgRecvWr());
  msg->message_bytes = static_cast<uint16_t>(FLAGS_rdma_message_bytes);

  const std::vector<QueuePairInfo> infos = qps.GetInfos();
  std::ranges::copy(infos, msg->qp_infos);
}

HandshakeHandler::HandshakeHandler(Listener* listener,
                                   SessionManager* session_manager,
                                   ServiceRegistry* service_registry)
    : listener_(listener),
      session_manager_(session_manager),
      service_registry_(service_registry) {}

Future<> HandshakeHandler::Handshake(
    Controller* cntl, const pb::blockcache::HandshakeRequest* request,
    pb::blockcache::HandshakeResponse* response) {
  HandshakeMsg peer;
  if (!peer.FromPb(request->endpoint_info())) {
    cntl->SetFailed("the peer's handshake does not decode");
    co_return;
  }

  HandshakeMsg local;
  const Status status = co_await Establish(peer, &local);
  if (!status.ok()) {
    cntl->SetFailed(status.ToString());
    co_return;
  }

  local.ToPb(response->mutable_endpoint_info());
}

Future<Status> HandshakeHandler::Establish(HandshakeMsg peer,
                                           HandshakeMsg* local) {
  Gate::Holder holder = session_manager_->EnterGate();
  if (!holder.ok()) {
    co_return ToStatus(ECANCELED, "accept a connection: context is stopping");
  }

  StatusOr<ConnectionUPtr> conn = listener_->Accept(peer, local);
  if (!conn.ok()) {
    co_return conn.status();
  }

  auto session = std::make_unique<Session>(
      std::move(conn).value(), service_registry_,
      [this] { return session_manager_->EnterGate(); });
  co_return co_await session_manager_->AddSession(std::move(session));
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
