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

#include "blockcache/net/rdma/domain.h"

#include <absl/cleanup/cleanup.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/core/reactor/timer.h"

namespace dingofs {
namespace blockcache {

static SlabPool::Option PoolOption(const RdmaOption& option) {
  SlabPool::Option pool;
  pool.superblock_count = option.pool_superblocks();
  pool.numa_node = memory::LocalNumaNode();
  return pool;
}

RdmaDomain::~RdmaDomain() {
  CHECK(conns_.empty()) << "RdmaDomain destroyed before Shutdown()";
}

RdmaDomain::RdmaDomain(const RdmaOption& option, RequestHandler* handler)
    : option_(option), pool_(PoolOption(option)), handler_(handler) {}

StatusOr<std::unique_ptr<RdmaDomain>> RdmaDomain::Create(
    const RdmaOption& option, RequestHandler* handler) {
  const char* reason = CheckOption(option);
  if (reason != nullptr) {
    return ToStatus(EINVAL, reason);
  }
  if (handler == nullptr) {
    return ToStatus(EINVAL, "an domain needs a handler table");
  }
  std::unique_ptr<RdmaDomain> domain(new RdmaDomain(option, handler));
  Status status = domain->Init();
  if (!status.ok()) {
    return status;
  }
  return domain;
}

Status RdmaDomain::Init() {
  // The shard that will own every queue pair, completion queue and
  // registration on this domain also owns the context they hang off, so
  // nothing inside the provider is shared with another core. Endpoints opened
  // off a shard -- tests, tools -- fall back to one shared context.
  const unsigned shard =
      HasReactor() ? ThisShardId() : verbs::Device::kAnyShard;
  StatusOr<verbs::Device*> device = verbs::Device::Open(
      verbs::DeviceOption{option_.device, option_.port_num, option_.gid_index},
      shard);
  if (!device.ok()) {
    return device.status();
  }
  device_ = device.value();

  StatusOr<verbs::CompletionChannel> channel =
      verbs::CompletionChannel::Create(*device_);
  if (!channel.ok()) {
    return channel.status();
  }
  comp_channel_ = std::move(channel).value();

  StatusOr<verbs::CompletionQueue> cq = verbs::CompletionQueue::Create(
      *device_, option_.cq_entries, &comp_channel_);
  if (!cq.ok()) {
    return cq.status();
  }
  cq_ = std::move(cq).value();

  DINGOFS_RETURN_NOT_OK(
      memory_.Init(device_->pd(), pool_.base(), pool_.total_bytes()));

  // By default the whole pool is what peers may read and write one-sidedly;
  // callers narrow this with Expose().
  exposed_ = ExposedRegion{reinterpret_cast<uint64_t>(pool_.base()),
                           pool_.total_bytes(), memory_.pool_rkey(), 0};
  poller_.emplace(this);
  return Status::OK();
}

// One serving domain per shard. A thread_local rather than a member of
// anything above, because the shard IS the scope: the server transport
// publishes on the shard that owns the domain, and only that shard's
// requests ever read it.
static thread_local RdmaDomain* tls_serving_domain = nullptr;

Future<> RdmaDomain::Shutdown() {
  if (stopped_) {
    co_return;
  }
  stopped_ = true;

  // Unpublish before anything below suspends: a handshake that arrives from
  // here on must see "not serving", never a dying domain.
  sweep_timer_.Cancel();
  if (tls_serving_domain == this) {
    tls_serving_domain = nullptr;
  }

  co_await conns_.ShutdownAll(&gate_);

  if (poller_.has_value()) {
    co_await poller_->Disarm();
    poller_.reset();
  }
}

Future<StatusOr<RdmaConnection*>> RdmaDomain::Connect(Client& bootstrap,
                                                      uint64_t route_hint) {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return ToStatus(ECANCELED, "connect: the domain is stopping");
  }

  StatusOr<std::unique_ptr<RdmaConnection>> made = co_await NewConnection();
  if (!made.ok()) {
    co_return made.status();
  }
  std::unique_ptr<RdmaConnection> conn = std::move(made).value();
  bool registered = false;
  absl::Cleanup give_back = [this, &registered] {
    if (!registered) {
      cq_.Unreserve(ConnectionWrs());
    }
  };

  HandshakeMsg mine;
  conn->FillHandshake(&mine, exposed_);

  // One rpc carries the whole exchange, and the reply IS the old ready
  // barrier: the responder reaches RTS before it answers, so nothing can be
  // transmitted to a queue pair that is not yet receiving.
  StatusOr<Reply> reply = co_await bootstrap.Call(
      kOpRdmaHandshake,
      std::string_view(reinterpret_cast<const char*>(&mine), sizeof(mine)),
      Body::None(), route_hint);

  HandshakeMsg peer;
  Status ready = reply.ok() ? Status::OK() : reply.status();
  if (ready.ok() && !reply.value().accepted()) {
    ready = ToStatus(ECONNREFUSED, "handshake with the peer");
  }
  if (ready.ok() && reply.value().payload().size() != sizeof(peer)) {
    // A peer speaking another version fails here, on size, before anything
    // is reinterpreted.
    ready = ToStatus(EPROTO, "read the handshake reply");
  }
  if (ready.ok()) {
    std::memcpy(&peer, reply.value().payload().data(), sizeof(peer));
    ready = ApplyHandshake(conn.get(), peer);
  }
  if (!ready.ok()) {
    conn->OnError(ToStatus(ECONNABORTED, "complete the handshake"));
    co_await conn->Drain();
    co_return ready;
  }

  RdmaConnection* raw = conn.get();
  if (!conns_.Add(std::move(conn))) {
    raw->OnError(ToStatus(ECANCELED, "register a connection: stopping"));
    co_await raw->Drain();
    co_return ToStatus(ECANCELED, "complete a handshake: domain stopping");
  }
  registered = true;
  raw->StartKeepalive(option_.ping_interval_ns);
  co_return raw;
}

Future<StatusOr<RdmaConnection*>> RdmaDomain::Accept(const HandshakeMsg& peer,
                                                     HandshakeMsg* mine) {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return ToStatus(ECANCELED, "accept: the domain is stopping");
  }

  StatusOr<std::unique_ptr<RdmaConnection>> made = co_await NewConnection();
  if (!made.ok()) {
    co_return made.status();
  }
  std::unique_ptr<RdmaConnection> conn = std::move(made).value();
  bool registered = false;
  absl::Cleanup give_back = [this, &registered] {
    if (!registered) {
      cq_.Unreserve(ConnectionWrs());
    }
  };

  conn->FillHandshake(mine, exposed_);
  Status ready = ApplyHandshake(conn.get(), peer);
  if (!ready.ok()) {
    conn->OnError(ToStatus(ECONNABORTED, "complete the handshake"));
    co_await conn->Drain();
    co_return ready;
  }

  RdmaConnection* raw = conn.get();
  if (!conns_.Add(std::move(conn))) {
    raw->OnError(ToStatus(ECANCELED, "register a connection: stopping"));
    co_await raw->Drain();
    co_return ToStatus(ECANCELED, "complete a handshake: domain stopping");
  }
  registered = true;
  // No keepalive here: an accepted connection is the idle sweep's to watch,
  // fed by the initiator's pings.
  co_return raw;
}

Future<> RdmaDomain::Retire(RdmaConnection* conn) {
  co_await conn->Shutdown();
  // Shutdown only marks it dead; the table is what drops it, and the same
  // pass is what gives the completion queue its budget back.
  const size_t reaped = co_await conns_.ReapDead();
  cq_.Unreserve(static_cast<uint32_t>(reaped) * ConnectionWrs());
}

RdmaDomain* RdmaDomain::Serving() { return tls_serving_domain; }

void RdmaDomain::StartServing() {
  CHECK(tls_serving_domain == nullptr) << "two endpoints serving one shard";
  tls_serving_domain = this;
  sweep_timer_.SetCallback([this] { (void)Sweep(); });
  sweep_timer_.ArmPeriodic(std::chrono::nanoseconds(option_.sweep_period_ns()));
}

Future<> RdmaDomain::Sweep() {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return;
  }
  conns_.MarkIdleDead(TimestampNs() - option_.idle_timeout_ns);
  // The reap that used to happen only when a new handshake arrived -- a quiet
  // shard never reclaimed its dead until this timer.
  const size_t reaped = co_await conns_.ReapDead();
  cq_.Unreserve(static_cast<uint32_t>(reaped) * ConnectionWrs());
}

Future<StatusOr<std::unique_ptr<RdmaConnection>>> RdmaDomain::NewConnection() {
  // Retire what already died before booking for one more: this is where the
  // freed budget is wanted.
  const size_t reaped = co_await conns_.ReapDead();
  cq_.Unreserve(static_cast<uint32_t>(reaped) * ConnectionWrs());

  const uint32_t booked = ConnectionWrs();
  Status status = cq_.Reserve(booked, device_->max_cqe());
  if (!status.ok()) {
    co_return status;
  }
  auto conn = std::make_unique<RdmaConnection>(this);
  status = conn->Init();
  if (!status.ok()) {
    // Whatever Init managed to post must flush before the connection dies.
    conn->OnError(ToStatus(ECONNABORTED, "initialize a connection"));
    co_await conn->Drain();
    cq_.Unreserve(booked);
    co_return status;
  }
  co_return std::move(conn);
}

Status RdmaDomain::ApplyHandshake(RdmaConnection* conn,
                                  const HandshakeMsg& peer) {
  const char* reason =
      CheckHandshake(peer, option_.total_lanes(), device_->port().link_layer);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to handshake with peer: " << reason;
    return ToStatus(EPROTO, reason);
  }
  return conn->ApplyPeer(peer);
}

}  // namespace blockcache
}  // namespace dingofs
