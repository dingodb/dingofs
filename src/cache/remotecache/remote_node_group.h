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
 * Created Date: 2025-06-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_

#include "cache/common/proto.h"
#include "cache/common/type.h"
#include "cache/remotecache/remote_node.h"
#include "cache/remotecache/remote_node_manager.h"
#include "cache/utils/con_hash.h"
#include "cache/utils/context.h"
#include "options/cache/tiercache.h"

namespace dingofs {
namespace cache {

class CacheUpstream {
 public:
  CacheUpstream();

  CacheUpstream(const PBCacheGroupMembers& members,
                RemoteBlockCacheOption option);

  Status Init();

  RemoteNodeSPtr GetNode(const std::string& key);

  bool IsDiff(const PBCacheGroupMembers& members) const;

 private:
  std::vector<uint64_t> CalcWeights(const PBCacheGroupMembers& members);

  std::string MemberKey(const PBCacheGroupMember& member) const;

  PBCacheGroupMembers members_;
  RemoteBlockCacheOption option_;
  std::shared_ptr<ConHash> chash_;
  std::unordered_map<std::string, RemoteNodeSPtr> nodes_;
};

using CacheUpstreamSPtr = std::shared_ptr<CacheUpstream>;

class RemoteNodeGroup final : public RemoteNode {
 public:
  explicit RemoteNodeGroup(RemoteBlockCacheOption option);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, size_t block_size) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key,
               const Block& block) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length) override;

 private:
  Status OnMemberLoad(const PBCacheGroupMembers& members);

  RemoteNodeSPtr GetNode(const BlockKey& key);

  std::atomic<bool> running_;
  BthreadRWLock rwlock_;  // protect upstream_
  RemoteBlockCacheOption option_;
  CacheUpstreamSPtr upstream_;
  RemoteNodeManagerUPtr node_manager_;
};

using RemoteNodeGorupSPtr = std::shared_ptr<RemoteNodeGroup>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_GROUP_H_
