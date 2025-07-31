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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_GROUP_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_GROUP_H_

#include "cache/blockcache/block_cache.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/remotecache/remote_cache_node_manager.h"
#include "cache/remotecache/upstream.h"
#include "cache/utils/context.h"
#include "metrics/cache/remotecache/remote_cache_node_group_metric.h"
#include "options/cache/tiercache.h"

namespace dingofs {
namespace cache {

class RemoteCacheNodeGroup final : public RemoteCacheNode {
 public:
  explicit RemoteCacheNodeGroup(RemoteBlockCacheOption option);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, RangeOption option) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key,
               const Block& block) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length) override;

 private:
  std::atomic<bool> running_;
  RemoteBlockCacheOption option_;
  UpstreamUPtr upstream_;
  RemoteCacheNodeManagerUPtr node_manager_;
  RemoteCacheCacheNodeGroupMetricSPtr metric_;
};

using RemoteNodeGorupSPtr = std::shared_ptr<RemoteCacheNodeGroup>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_GROUP_H_
