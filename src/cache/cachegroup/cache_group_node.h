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
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_

#include "cache/blockcache/block_cache.h"
#include "cache/cachegroup/async_cacher.h"
#include "cache/cachegroup/cache_group_node_heartbeat.h"
#include "cache/cachegroup/cache_group_node_member.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

class CacheGroupNode {
 public:
  virtual ~CacheGroupNode() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
                     PutOption option = PutOption()) = 0;
  virtual Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer,
                       RangeOption option = RangeOption()) = 0;
  virtual Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                       CacheOption option = CacheOption()) = 0;
  virtual Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                          PrefetchOption option = PrefetchOption()) = 0;
};

using CacheGroupNodeSPtr = std::shared_ptr<CacheGroupNode>;

class CacheGroupNodeImpl final : public CacheGroupNode {
 public:
  explicit CacheGroupNodeImpl(CacheGroupNodeOption option);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
             PutOption option = PutOption()) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer,
               RangeOption option = RangeOption()) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
               CacheOption option = CacheOption()) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                  PrefetchOption option = PrefetchOption()) override;

 private:
  void RewriteCacheDir();
  Status InitBlockCache();

  Status RangeCachedBlock(ContextSPtr ctx, const BlockKey& key, off_t offset,
                          size_t length, IOBuffer* buffer, RangeOption option);
  Status RangeStorage(ContextSPtr ctx, const BlockKey& key, off_t offset,
                      size_t length, IOBuffer* buffer, RangeOption option);

  void AddCacheHitCount(int64_t count);
  void AddCacheMissCount(int64_t count);

 private:
  std::atomic<bool> running_;
  CacheGroupNodeOption option_;
  std::shared_ptr<stub::rpcclient::MDSBaseClient> mds_base_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;
  CacheGroupNodeMemberSPtr member_;
  BlockCacheSPtr block_cache_;
  AsyncCacherUPtr async_cacher_;
  CacheGroupNodeHeartbeatUPtr heartbeat_;
  StoragePoolSPtr storage_pool_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
