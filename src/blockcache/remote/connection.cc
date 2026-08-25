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

#include "blockcache/remote/connection.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/route.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/infiniband/client/channel.h"
#include "blockcache/net/brpc/brpc_channel.h"
#include "blockcache/net/controller.h"

namespace dingofs {
namespace blockcache {

thread_local unsigned tls_open_connections = 0;

static ChannelOption GenChannelOption(const NodeConnection::Option& option) {
  ChannelOption channel_option;
  channel_option.server = option.server;
  channel_option.tag =
      "remote-" + std::to_string(ThisShardId()) + "-" + option.node_id;
  if (option.use_rdma) {
    channel_option.route_key =
        HintForShard(option.remote_shard, option.remote_shard_count);
    channel_option.expected_shard = option.remote_shard;
  }
  return channel_option;
}

static ChannelUPtr NewChannel(bool use_rdma) {
  if (use_rdma) {
    return std::make_unique<infiniband::Channel>();
  }
  return std::make_unique<BrpcChannel>(ThisShardId());
}

NodeProber::NodeProber(std::string node_id, std::string address)
    : node_id_(std::move(node_id)), address_(std::move(address)) {}

Future<Status> NodeProber::Start() {
  if (ready_) {
    co_return Status::OK();
  }

  LOG(INFO) << "Nodeprober for " << node_id_ << " at " << address_
            << " is starting...";

  const Status status = co_await probing_.Do([this] { return GetNodeInfo(); });

  LOG_IF(INFO, status.ok()) << "Successfully start NodeProber for " << node_id_;
  co_return status;
}

Future<> NodeProber::Shutdown() {
  LOG(INFO) << "NodeProber for " << node_id_ << " is shutting down";
  ready_ = false;
  LOG(INFO) << "Successfully shutdown NodeProber for " << node_id_;
  co_return;
}

Future<Status> NodeProber::GetNodeInfo() {
  ChannelUPtr channel = NewChannel(false);
  Status status = co_await channel->Init(
      GenChannelOption({.node_id = node_id_, .server = address_}));
  if (!status.ok()) {
    co_await channel->Shutdown();
    co_return status;
  }

  CacheStub stub(channel.get());
  Controller cntl;
  pb::blockcache::GetNodeInfoRequest request;
  pb::blockcache::GetNodeInfoResponse response;
  status = co_await stub.GetNodeInfo(&cntl, &request, &response);
  co_await channel->Shutdown();
  if (!status.ok()) {
    co_return status;
  }
  status = ToStatus(response.status());
  if (!status.ok()) {
    co_return status;
  }

  remote_shard_count_ = std::max(1U, response.shards());
  use_rdma_ = FLAGS_remote_rdma && response.rdma_enabled();

  const unsigned shard = ThisShardId();
  if (use_rdma_) {
    remote_shard_range_ =
        GetRemoteShardRange(shard, ShardCount(), remote_shard_count_);
  } else {
    remote_shard_range_ = RemoteShardRange{.base = 0, .count = 1};
  }
  ready_ = true;

  LOG(INFO) << "Cache node " << node_id_ << " at " << address_ << " serves "
            << remote_shard_count_ << " shard(s) over "
            << (use_rdma_ ? "rdma" : "tcp") << "; shard " << shard
            << " owns remote shard(s) [" << remote_shard_range_.base << ","
            << remote_shard_range_.base + remote_shard_range_.count << ")";
  co_return Status::OK();
}

NodeProber::RemoteShardRange NodeProber::GetRemoteShardRange(
    unsigned local_shard_id, unsigned local_shards, unsigned remote_shards) {
  remote_shards = std::max(1U, remote_shards);
  if (remote_shards == 1 || local_shards <= 1 ||
      local_shard_id >= local_shards) {
    return RemoteShardRange{.base = 0, .count = remote_shards};
  }

  const uint64_t first = FirstHintOf(local_shard_id, local_shards);
  uint64_t last;
  if (local_shard_id + 1 == local_shards) {
    last = UINT64_MAX;
  } else {
    last = FirstHintOf(local_shard_id + 1, local_shards) - 1;
  }
  const unsigned base = ShardOf(first, remote_shards);
  const unsigned highest = ShardOf(last, remote_shards);
  return RemoteShardRange{.base = base, .count = highest - base + 1};
}

uint64_t NodeProber::FirstHintOf(unsigned shard, unsigned shards) {
  if (shard == 0 || shards <= 1) {
    return 0;
  }
  const __uint128_t scaled =
      (static_cast<__uint128_t>(shard) << 64) + shards - 1;
  return static_cast<uint64_t>(scaled / shards);
}

NodeConnection::NodeConnection(Option option) : option_(std::move(option)) {}

Future<Status> NodeConnection::Open() {
  if (IsConnected()) {
    co_return Status::OK();
  }
  co_return co_await opening_.Do([this] { return Establish(); });
}

Future<> NodeConnection::Close() {
  if (stub_ != nullptr && option_.use_rdma) {
    --tls_open_connections;
  }
  stub_.reset();
  if (channel_ == nullptr) {
    co_return;
  }
  const ChannelUPtr channel = std::move(channel_);
  co_await channel->Shutdown();
}

bool NodeConnection::IsConnected() const {
  return stub_ != nullptr && channel_ != nullptr && channel_->Alive();
}

Future<Status> NodeConnection::Establish() {
  if (IsConnected()) {
    co_return Status::OK();
  } else if (stub_ != nullptr) {
    co_await Close();
  }

  ChannelUPtr channel = NewChannel(option_.use_rdma);
  const Status status = co_await channel->Init(GenChannelOption(option_));
  if (!status.ok()) {
    co_await channel->Shutdown();
    co_return status;
  }
  channel_ = std::move(channel);
  stub_ = std::make_unique<CacheStub>(channel_.get());

  if (option_.use_rdma) {
    LOG_IF(ERROR, ++tls_open_connections > FLAGS_rdma_max_connections)
        << "Shard " << ThisShardId() << " now holds " << tls_open_connections
        << " rdma connections but its registered pool is sized for "
        << FLAGS_rdma_max_connections
        << "; raise --rdma_max_connections or connections will fail to "
           "carve a frame buffer";
  }
  co_return Status::OK();
}

NodeConnections::NodeConnections(CacheGroupMember member)
    : member_(std::move(member)),
      prober_(std::make_unique<NodeProber>(member_.id, Address())) {}

NodeConnections::~NodeConnections() {
  LOG_IF(WARNING, running_) << "NodeConnections destroyed without Shutdown()";
}

Future<Status> NodeConnections::Start() {
  running_ = true;

  LOG(INFO) << *this << " is starting...";

  Status status = co_await prober_->Start();
  if (!status.ok()) {
    co_return status;
  }

  NewConnections();
  status = co_await OpenAll();

  LOG_IF(INFO, status.ok()) << "Successfully start " << *this;
  co_return status;
}

Future<> NodeConnections::Shutdown() {
  if (!running_) {
    co_return;
  }

  running_ = false;

  LOG(INFO) << *this << " is shutting down...";

  co_await CloseAll();
  co_await prober_->Shutdown();

  LOG(INFO) << "Successfully shutdown " << *this;
}

Future<StatusOr<CacheStub*>> NodeConnections::Get(uint64_t key) {
  if (!connections_.empty()) {
    NodeConnection& conn = GetConnection(key);
    if (conn.IsConnected()) {
      return MakeReadyFuture<StatusOr<CacheStub*>>(conn.stub());
    }
  }
  return DialThenGet(key);
}

void NodeConnections::NewConnections() {
  if (!connections_.empty()) {
    return;
  }

  const auto& remote = prober_->remote_shards();
  connections_.reserve(remote.count);
  for (unsigned i = 0; i < remote.count; ++i) {
    NodeConnection::Option option;
    option.node_id = member_.id;
    option.server = Address();
    option.remote_shard = remote.base + i;
    option.remote_shard_count = prober_->remote_shard_count();
    option.use_rdma = prober_->use_rdma();
    connections_.push_back(std::make_unique<NodeConnection>(std::move(option)));
  }
}

Future<Status> NodeConnections::OpenAll() {
  for (const NodeConnectionUPtr& conn : connections_) {
    const Status status = co_await conn->Open();
    if (!status.ok()) {
      co_return status;
    }
  }
  co_return Status::OK();
}

Future<> NodeConnections::CloseAll() {
  for (const NodeConnectionUPtr& conn : connections_) {
    co_await conn->Close();
  }
  connections_.clear();
}

NodeConnection& NodeConnections::GetConnection(uint64_t key) {
  if (!prober_->use_rdma()) {
    return *connections_[0];
  }

  const auto& remote = prober_->remote_shards();
  const unsigned remote_shard = ShardOf(key, prober_->remote_shard_count());
  if (!remote.Contains(remote_shard)) {
    LOG_EVERY_N(WARNING, 1000)
        << "A request for remote shard " << remote_shard << " reached shard "
        << ThisShardId() << ", which owns [" << remote.base << ","
        << remote.base + remote.count << "); the node forwards it one hop";
    return *connections_[remote_shard < remote.base ? 0 : remote.count - 1];
  }
  return *connections_[remote_shard - remote.base];
}

Future<StatusOr<CacheStub*>> NodeConnections::DialThenGet(uint64_t key) {
  if (!running_) {
    co_return Status::CacheDown("the connection to this cache node is closed");
  }

  const Status s = co_await prober_->Start();
  if (!s.ok()) {
    co_return s;
  }

  NewConnections();

  NodeConnection& conn = GetConnection(key);
  const Status status = co_await conn.Open();
  if (!status.ok()) {
    co_return status;
  }

  if (!conn.IsConnected()) {
    co_return Status::CacheDown("the connection to this cache node is closed");
  }

  co_return conn.stub();
}

std::string NodeConnections::Address() const {
  return member_.ip + ":" + std::to_string(member_.port);
}

std::ostream& operator<<(std::ostream& os, const NodeConnections& connections) {
  os << "NodeConnections{id=" << connections.member_.id
     << " endpoint=" << connections.Address() << " shard=" << ThisShardId()
     << "}";
  return os;
}

}  // namespace blockcache
}  // namespace dingofs
