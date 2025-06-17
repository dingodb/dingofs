/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_IMPL_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_IMPL_H_

#include <brpc/channel.h>

#include "cache/config/tiercache.h"
#include "cache/remotecache/remote_node.h"
#include "cache/remotecache/rpc_client.h"
#include "cache/utils/context.h"
#include "cache/utils/state_machine.h"

namespace dingofs {
namespace cache {

class RemoteNodeImpl final : public RemoteNode {
 public:
  RemoteNodeImpl(const PBCacheGroupMember& member,
                 RemoteBlockCacheOption option);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, size_t block_size) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key,
               const Block& block) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length) override;

 private:
  Status CheckHealth() const;
  Status CheckStatus(Status status);

  std::atomic<bool> running_;
  const PBCacheGroupMember member_info_;
  RPCClientUPtr rpc_;
  StateMachineUPtr state_machine_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_IMPL_H_
