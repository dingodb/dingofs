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

#include "blockcache/net/rdma/transport.h"

#include <glog/logging.h>

#include <cerrno>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/transport.h"

namespace dingofs {
namespace blockcache {

RdmaTransport::~RdmaTransport() { Shutdown(); }

// Everything the domain owns -- device handles, completion queue, dma pool,
// registrations, the cq poller -- is created here, on the core that will use
// it, and never touched from anywhere else.
static Future<Status> OpenDomainOnThisShard(std::unique_ptr<RdmaDomain>* slot,
                                            RdmaOption option,
                                            RequestHandler* handler) {
  StatusOr<std::unique_ptr<RdmaDomain>> made =
      RdmaDomain::Create(option, handler);
  if (!made.ok()) {
    co_return made.status();
  }
  std::unique_ptr<RdmaDomain> domain = std::move(made).value();
  // Publishing is what makes the handshake verb find this domain; there is no
  // listener -- the handshake arrives as an rpc on whatever transport serves
  // this process.
  domain->StartServing();
  *slot = std::move(domain);
  co_return Status::OK();
}

// Drained AND destroyed on the owning shard: the queue pairs, the completion
// queue and the poller registration all belong to that reactor, so tearing
// them down from another thread is wrong however the memory was allocated.
static Future<> CloseDomainOnThisShard(std::unique_ptr<RdmaDomain>* slot) {
  if (*slot != nullptr) {
    co_await (*slot)->Shutdown();
    slot->reset();
  }
}

Status RdmaTransport::Start(const ServerContext& context) {
  CHECK(!started_) << "RdmaTransport already started";
  CHECK_EQ(context.shard_count(), ShardCount()) << "one handler per shard";

  domains_.resize(context.shard_count());

  Status status =
      RunOnAllAndWait([this, &context](unsigned shard) -> Future<Status> {
        return OpenDomainOnThisShard(&domains_[shard], option_,
                                     context.HandlerOf(shard));
      });
  if (!status.ok()) {
    StopDomains();  // whatever came up; the slots that failed are null
    return status;
  }

  started_ = true;
  return Status::OK();
}

void RdmaTransport::Shutdown() {
  if (!started_) {
    return;
  }
  started_ = false;
  StopDomains();
}

Status RdmaTransport::RegisterShardMemory(unsigned shard, void* addr,
                                          size_t length) {
  CHECK(started_) << "RdmaTransport is not started";
  DCHECK_EQ(shard, ThisShardId()) << "register with the owning shard's device";
  StatusOr<const verbs::MemoryRegion*> mr =
      domains_[shard]->memory().Register(addr, length);
  return mr.ok() ? Status::OK() : mr.status();
}

void RdmaTransport::StopDomains() {
  if (domains_.empty()) {
    return;
  }
  RunOnAllAndWait([this](unsigned shard) -> Future<> {
    return CloseDomainOnThisShard(&domains_[shard]);
  });
  domains_.clear();
}

}  // namespace blockcache
}  // namespace dingofs
