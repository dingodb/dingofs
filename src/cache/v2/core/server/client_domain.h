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

#ifndef DINGOFS_CACHE_V2_CORE_SERVER_CLIENT_DOMAIN_H_
#define DINGOFS_CACHE_V2_CORE_SERVER_CLIENT_DOMAIN_H_

#include <memory>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/handler.h"
#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/option.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// An rdma domain that only ever dials: it serves nothing, so it carries
// the handler that says so, and it registers this shard's buffer pool once
// so every attachment sent over it is already pinned.
//
// The shard-wide instance is the point. An domain is expensive -- a
// completion queue, a registered pool, a poller -- while a connection is
// not, and one domain's ConnectionTable holds as many as wanted. A caller
// that speaks to a whole cache group opens one connection per peer shard it
// routes to; one domain each would be unaffordable.
class ClientDomain {
 public:
  // One of its own, for a caller that dials once and wants no shared state.
  static StatusOr<std::unique_ptr<ClientDomain>> Create(
      const RdmaOption& option);

  // Creates the shard-wide instance on THIS shard, refcounted: several
  // components on one shard may want it, and the last one out closes it.
  static Future<Status> InitOnThisShard(RdmaOption option);
  static Future<> ShutdownOnThisShard();

  // The same, fanned out from a thread outside the runtime.
  static Status InitOnAllShards(const RdmaOption& option);
  // Every Channel dialled over them must be shut down first.
  static void ShutdownOnAllShards();

  // Pins a caller's own memory into every shard's protection domain -- one
  // ibv_reg_mr per shard, because each shard has its own. Shards without a
  // domain are skipped. External thread, after the domains are up.
  static Status RegisterOnAllShards(void* base, size_t bytes);

  ~ClientDomain();

  ClientDomain(const ClientDomain&) = delete;
  ClientDomain& operator=(const ClientDomain&) = delete;

  // Belongs to its shard, like everything else here.
  Future<> Shutdown();

  // This shard's, or null when none was created -- in which case a Channel
  // falls back to owning its domain.
  static RdmaDomain* DomainOfThisShard();

  RdmaDomain* domain() const { return domain_.get(); }

 private:
  ClientDomain() = default;

  // Declared before the domain so it outlives it.
  std::unique_ptr<RequestHandler> handler_;
  std::unique_ptr<RdmaDomain> domain_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_SERVER_CLIENT_DOMAIN_H_
