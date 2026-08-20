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

#include "blockcache/net/server/client_domain.h"

#include <glog/logging.h>

#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/request.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {

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

static Future<> AdoptOnThisShard(verbs::MemoryRegion* region) {
  RdmaDomain* domain = ClientDomain::DomainOfThisShard();
  if (domain != nullptr) {
    domain->memory().Adopt(std::move(*region));
  }
  co_return;
}

Status ClientDomain::RegisterOnAllShards(void* base, size_t bytes) {
  if (base == nullptr || bytes == 0) {
    return Status::OK();
  }

  const unsigned shards = ShardCount();
  std::vector<ibv_pd*> pds(shards, nullptr);
  for (unsigned shard = 0; shard < shards; ++shard) {
    pds[shard] = RunOnAndWait(shard, []() -> Future<ibv_pd*> {
      RdmaDomain* domain = DomainOfThisShard();
      co_return domain == nullptr ? nullptr : domain->device().pd();
    });
  }

  std::vector<std::optional<verbs::MemoryRegion>> regions(shards);
  std::vector<Status> results(shards);
  std::vector<std::thread> pinners;
  for (unsigned shard = 0; shard < shards; ++shard) {
    if (pds[shard] == nullptr) {
      continue;
    }
    pinners.emplace_back([&, shard] {
      StatusOr<verbs::MemoryRegion> mr =
          verbs::MemoryRegion::Register(pds[shard], base, bytes);
      if (mr.ok()) {
        regions[shard].emplace(std::move(mr).value());
      } else {
        results[shard] = mr.status();
      }
    });
  }
  for (std::thread& pinner : pinners) {
    pinner.join();
  }
  for (const Status& status : results) {
    if (!status.ok()) {
      return status;
    }
  }

  for (unsigned shard = 0; shard < shards; ++shard) {
    if (!regions[shard].has_value()) {
      continue;
    }
    verbs::MemoryRegion* region = &*regions[shard];
    RunOnAndWait(shard,
                 [region]() -> Future<> { return AdoptOnThisShard(region); });
  }
  return Status::OK();
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

}  // namespace blockcache
}  // namespace dingofs
