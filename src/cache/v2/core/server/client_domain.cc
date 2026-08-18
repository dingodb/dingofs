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

#include "cache/v2/core/server/client_domain.h"

#include <glog/logging.h>

#include <utility>

#include "cache/v2/core/memory/buffer.h"
#include "cache/v2/core/net/request.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/runtime/smp.h"

namespace dingofs {
namespace cache {
namespace v2 {

namespace {

// Rejects stray frames on a dialling domain instead of crashing.
class RejectAll final : public RequestHandler {
 public:
  Future<Status> Serve(Request& request) override {
    return request.Reply(kReplyBadOpcode, {});
  }
};

thread_local std::unique_ptr<ClientDomain> tls_client_domain;
// A domain is opened per shard but wanted by several components on it, so it
// is refcounted: the last one out closes it.
thread_local unsigned tls_client_domain_refs = 0;

}  // namespace

StatusOr<std::unique_ptr<ClientDomain>> ClientDomain::Create(
    const RdmaOption& option) {
  auto self = std::unique_ptr<ClientDomain>(new ClientDomain());
  self->handler_ = std::make_unique<RejectAll>();

  StatusOr<std::unique_ptr<RdmaDomain>> made =
      RdmaDomain::Create(option, self->handler_.get());
  if (!made.ok()) {
    return made.status();
  }
  self->domain_ = std::move(made).value();

  // The shard's buffer pool becomes one MR, so attachments are pre-registered.
  if (SlabPool* pool = BufferPool::LocalPool(); pool != nullptr) {
    StatusOr<const verbs::MemoryRegion*> mr =
        self->domain_->memory().Register(pool->base(), pool->total_bytes());
    if (!mr.ok()) {
      // Shutdown suspends; the caller is not a coroutine, so leak-free
      // teardown here means letting the domain's own destructor run.
      self->domain_.reset();
      return mr.status();
    }
  }
  return self;
}

Future<Status> ClientDomain::InitOnThisShard(RdmaOption option) {
  if (tls_client_domain != nullptr) {
    ++tls_client_domain_refs;
    co_return Status::OK();
  }
  StatusOr<std::unique_ptr<ClientDomain>> made = ClientDomain::Create(option);
  if (!made.ok()) {
    co_return made.status();
  }
  tls_client_domain = std::move(made).value();
  tls_client_domain_refs = 1;
  co_return Status::OK();
}

Future<> ClientDomain::ShutdownOnThisShard() {
  if (tls_client_domain == nullptr || --tls_client_domain_refs != 0) {
    co_return;
  }
  co_await tls_client_domain->Shutdown();
  tls_client_domain.reset();
}

Status ClientDomain::InitOnAllShards(const RdmaOption& option) {
  const Status status =
      RunOnAllAndWait([option](unsigned /*shard*/) -> Future<Status> {
        return InitOnThisShard(option);
      });
  if (!status.ok()) {
    ShutdownOnAllShards();
  }
  return status;
}

void ClientDomain::ShutdownOnAllShards() {
  RunOnAllAndWait(
      [](unsigned /*shard*/) -> Future<> { return ShutdownOnThisShard(); });
}

static Future<Status> RegisterOnThisShard(void* base, size_t bytes) {
  RdmaDomain* domain = ClientDomain::DomainOfThisShard();
  if (domain == nullptr) {
    co_return Status::OK();  // no rdma on this shard: nothing to pin
  }
  StatusOr<const verbs::MemoryRegion*> mr =
      domain->memory().Register(base, bytes);
  co_return mr.status();
}

Status ClientDomain::RegisterOnAllShards(void* base, size_t bytes) {
  if (base == nullptr || bytes == 0) {
    return Status::OK();
  }
  return RunOnAllAndWait([base, bytes](unsigned) -> Future<Status> {
    return RegisterOnThisShard(base, bytes);
  });
}

ClientDomain::~ClientDomain() {
  LOG_IF(WARNING, domain_ != nullptr)
      << "ClientDomain destroyed without Shutdown()";
}

Future<> ClientDomain::Shutdown() {
  if (domain_ != nullptr) {
    co_await domain_->Shutdown();
    domain_.reset();
  }
  handler_.reset();
}

RdmaDomain* ClientDomain::DomainOfThisShard() {
  return tls_client_domain == nullptr ? nullptr : tls_client_domain->domain();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
