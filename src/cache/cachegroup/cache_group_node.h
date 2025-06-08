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
#include "cache/cachegroup/async_cache.h"
#include "cache/cachegroup/cache_group_node_heartbeat.h"
#include "cache/cachegroup/cache_group_node_member.h"
#include "cache/config/config.h"
#include "cache/storage/storage_pool.h"

namespace dingofs {
namespace cache {

class CacheGroupNode {
 public:
  virtual ~CacheGroupNode() = default;

  virtual Status Start() = 0;
  virtual Status Stop() = 0;

  virtual Status Put(const BlockKey& key, const Block& block) = 0;
  virtual Status Range(const BlockKey& key, off_t offset, size_t length,
                       IOBuffer* buffer, size_t block_size) = 0;
  virtual Status Cache(const BlockKey& key, const Block& block) = 0;
  virtual Status Prefetch(const BlockKey& key, size_t length) = 0;
};

using CacheGroupNodeSPtr = std::shared_ptr<CacheGroupNode>;

class CacheGroupNodeImpl final : public CacheGroupNode {
 public:
  explicit CacheGroupNodeImpl(CacheGroupNodeOption option);

  Status Start() override;
  Status Stop() override;

  Status Put(const BlockKey& key, const Block& block) override;
  Status Range(const BlockKey& key, off_t offset, size_t length,
               IOBuffer* buffer, size_t block_size) override;
  Status Cache(const BlockKey& key, const Block& block) override;
  Status Prefetch(const BlockKey& key, size_t length) override;

 private:
  void RewriteCacheDir();
  Status InitBlockCache();

  Status RangeCachedBlock(const BlockKey& key, off_t offset, size_t length,
                          IOBuffer* buffer);
  Status RangeStorage(const BlockKey& key, off_t offset, size_t length,
                      IOBuffer* buffer, size_t block_size);

 private:
  std::atomic<bool> running_;
  CacheGroupNodeOption option_;
  std::shared_ptr<stub::rpcclient::MDSBaseClient> mds_base_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;
  CacheGroupNodeMemberSPtr member_;
  BlockCacheSPtr block_cache_;
  AsyncCacheUPtr async_cache_;
  CacheGroupNodeHeartbeatUPtr heartbeat_;
  StoragePoolSPtr storage_pool_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
