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

#include "blockcache/infiniband/client/dialer.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/core/runtime/shard_inbox.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/worker_pool.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Handshaker::Handshaker(unsigned shard, ChannelOption option,
                       const pb::blockcache::HandshakeRequest& request,
                       pb::blockcache::HandshakeResponse* response)
    : shard_(shard),
      option_(std::move(option)),
      request_(request),
      response_(response) {
  run = &Handshaker::OnWorker;
}

Future<Status> Handshaker::Handshake(
    const ChannelOption& option,
    const pb::blockcache::HandshakeRequest& request,
    pb::blockcache::HandshakeResponse* response) {
  auto* call = new Handshaker(ThisShardId(), option, request, response);
  Future<Status> done = call->promise_.GetFuture();
  CHECK_NOTNULL(GetGlobalWorkers())->Post(call);
  return done;
}

void Handshaker::OnWorker(InboxWork* base) {
  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(&tid, &attr, &Handshaker::OnBthread, base) !=
      0) {
    LOG(ERROR) << "Fail to start a handshake bthread; running it inline";
    OnBthread(base);
  }
}

void* Handshaker::OnBthread(void* arg) {
  auto* self = static_cast<Handshaker*>(arg);
  self->status_ = self->Call();
  self->run = &Handshaker::OnShard;
  if (!PostTo(self->shard_, self)) {
    LOG(ERROR) << "Fail to deliver a handshake reply to shard " << self->shard_
               << ": it is stopping";
    delete self;
  }
  return nullptr;
}

void Handshaker::OnShard(InboxWork* base) {
  auto* self = static_cast<Handshaker*>(base);
  self->promise_.SetValue(std::move(self->status_));
  delete self;
}

Status Handshaker::Call() {
  brpc::ChannelOptions options;
  options.connect_timeout_ms =
      static_cast<int32_t>(FLAGS_remote_connect_timeout_ms);
  options.timeout_ms = static_cast<int32_t>(FLAGS_remote_rpc_timeout_ms);
  options.max_retry = FLAGS_remote_rpc_max_retry;
  options.connection_type = "single";
  options.connection_group = option_.tag;

  brpc::Channel channel;
  if (channel.Init(option_.server.c_str(), &options) != 0) {
    return Status::NetError("Fail to connect to " + option_.server);
  }

  brpc::Controller cntl;
  pb::blockcache::InfinibandService_Stub stub(&channel);
  stub.Handshake(&cntl, &request_, response_, nullptr);
  if (cntl.Failed()) {
    return Status::NetError("Fail to handshake (brpc error " +
                            std::to_string(cntl.ErrorCode()) +
                            "): " + cntl.ErrorText());
  }
  return Status::OK();
}

Dialer::Dialer(Device* device, BufferPool* buffer_pool,
               CompletionQueue* completion_queue)
    : device_(device),
      buffer_pool_(buffer_pool),
      completion_queue_(completion_queue) {}

Dialer::~Dialer() {
  if (on_destroy_) {
    on_destroy_();
  }
}

Future<StatusOr<ConnectionUPtr>> Dialer::Dial(ChannelOption option) {
  Status status = ReserveCapacity();
  if (!status.ok()) {
    co_return status;
  }

  StatusOr<QueuePairGroup> qps =
      QueuePairGroup::Create(device_, completion_queue_);
  if (!qps.ok()) {
    co_return qps.status();
  }

  pb::blockcache::HandshakeRequest request;
  pb::blockcache::HandshakeResponse response;

  HandshakeMsg local;
  GetLocalInfo(qps.value(), &local);
  local.ToPb(request.mutable_endpoint_info());
  request.set_route_key(option.route_key);

  status = co_await Handshaker::Handshake(option, request, &response);
  if (!status.ok()) {
    co_return status;
  }

  HandshakeMsg peer;
  status = GetPeerInfo(response, &peer);
  if (!status.ok()) {
    co_return status;
  }

  status = qps.value().ModifyToReady({peer.qp_infos, peer.num_qps});
  if (!status.ok()) {
    co_return status;
  }

  if (option.expected_shard != UINT32_MAX &&
      peer.shard != option.expected_shard) {
    LOG(WARNING) << "The peer at " << option.server
                 << " answered the handshake from shard " << peer.shard
                 << ", not " << option.expected_shard
                 << "; its shard count changed under us";
  }

  ConnectionOption conn_option;
  conn_option.max_send_credits = peer.rpc_credits;
  conn_option.heartbeat_interval_ns =
      uint64_t{FLAGS_rdma_heartbeat_interval_s} * 1'000'000'000;
  co_return std::make_unique<Connection>(std::move(qps).value(), buffer_pool_,
                                         conn_option);
}

void Dialer::GetLocalInfo(const QueuePairGroup& qps,
                          HandshakeMsg* local) const {
  const PortInfo& port = device_->port_info();

  std::memset(local, 0, sizeof(*local));
  local->version = Protocol::kHandshakeVersion;
  local->shard = static_cast<uint16_t>(HasReactor() ? ThisShardId() : 0);
  local->num_qps = qps.qp_count();
  local->link_layer = static_cast<uint8_t>(port.link_layer);
  local->rpc_credits = static_cast<uint16_t>(Protocol::MsgRecvWr());
  local->message_bytes = static_cast<uint16_t>(FLAGS_rdma_message_bytes);

  const std::vector<QueuePairInfo> infos = qps.GetInfos();
  std::ranges::copy(infos, local->qp_infos);
}

Status Dialer::GetPeerInfo(const pb::blockcache::HandshakeResponse& response,
                           HandshakeMsg* peer) const {
  if (!peer->FromPb(response.endpoint_info())) {
    return ToStatus(EPROTO, "read the handshake reply");
  }

  auto qp_count = static_cast<uint8_t>(1 + FLAGS_rdma_bulk_qps);
  const char* reason = peer->Check(qp_count, device_->port_info().link_layer);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to handshake with peer: " << reason;
    return ToStatus(EPROTO, reason);
  }
  return Status::OK();
}

Status Dialer::ReserveCapacity() {
  DCHECK(!on_destroy_) << "one connection per dialer";
  const uint32_t work_requests =
      Protocol::MsgSendWr() + Protocol::MsgRecvWr() +
      (FLAGS_rdma_bulk_qps * FLAGS_rdma_bulk_send_wr);
  const Status status =
      completion_queue_->Reserve(work_requests, device_->max_cqe());
  if (status.ok()) {
    on_destroy_ = [this, work_requests] {
      completion_queue_->Unreserve(work_requests);
    };
  }
  return status;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
