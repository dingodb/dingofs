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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_TRANSPORT_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_TRANSPORT_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/option.h"
#include "cache/v2/core/net/transport.h"

namespace dingofs {
namespace cache {
namespace v2 {

// One RdmaDomain per shard, each created, served and destroyed on its own
// core. The handshake rpc's route hint picks the shard; nothing here
// re-routes afterwards, since a request owns shard-private wire resources.
class RdmaTransport final : public ServerTransport {
 public:
  explicit RdmaTransport(RdmaOption option) : option_(option) {}
  ~RdmaTransport() override;

  Status Start(const ServerContext& context) override;
  void Shutdown() override;
  const char* name() const override { return "rdma"; }

  // Makes memory outside the domain's own pool usable for one-sided
  // transfers. Call after Start(), ON `shard`: the device handles belong to
  // that core. The registration lasts until Shutdown().
  Status RegisterShardMemory(unsigned shard, void* addr, size_t length);

 private:
  // Destroys every domain that came up, each on its own shard.
  void StopDomains();

  RdmaOption option_;
  // Slot s is written only by shard s, under a blocking RunOnAllAndWait.
  std::vector<std::unique_ptr<RdmaDomain>> domains_;
  bool started_ = false;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_TRANSPORT_H_
