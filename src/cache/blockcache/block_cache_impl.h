/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_IMPL_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_IMPL_H_

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_uploader.h"
#include "cache/blockcache/cache_store.h"
#include "cache/config/config.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/infight_throttle.h"

namespace dingofs {
namespace cache {

class BlockCacheImpl final : public BlockCache {
 public:
  BlockCacheImpl(BlockCacheOption option, StorageSPtr storage);
  BlockCacheImpl(BlockCacheOption option, StoragePoolSPtr storage_pool);

  ~BlockCacheImpl() override = default;

  Status Init() override;
  Status Shutdown() override;

  Status Put(const BlockKey& key, const Block& block,
             PutOption option = PutOption()) override;
  Status Range(const BlockKey& key, off_t offset, size_t length,
               IOBuffer* buffer, RangeOption option = RangeOption()) override;
  Status Cache(const BlockKey& key, const Block& block,
               CacheOption option = CacheOption()) override;
  Status Prefetch(const BlockKey& key, size_t length,
                  PrefetchOption option = PrefetchOption()) override;

  void AsyncPut(const BlockKey& key, const Block& block, AsyncCallback cb,
                PutOption option = PutOption()) override;
  void AsyncRange(const BlockKey& key, off_t offset, size_t length,
                  IOBuffer* buffer, AsyncCallback cb,
                  RangeOption option = RangeOption()) override;
  void AsyncCache(const BlockKey& key, const Block& block, AsyncCallback cb,
                  CacheOption option = CacheOption()) override;
  void AsyncPrefetch(const BlockKey& key, size_t length, AsyncCallback cb,
                     PrefetchOption option = PrefetchOption()) override;

  bool IsCached(const BlockKey& key) const override;

 private:
  friend class BlockCacheBuilder;

  BlockCacheSPtr GetSelfSPtr() { return shared_from_this(); }

  Status StoragePut(const BlockKey& key, const IOBuffer& buffer);
  Status StorageRange(const BlockKey& key, off_t offset, size_t length,
                      IOBuffer* buffer);

  std::atomic<bool> running_;
  StoragePoolSPtr storage_pool_;
  CacheStoreSPtr store_;
  BlockCacheUploaderSPtr uploader_;
  InflightThrottleSPtr prefetch_throttle_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_IMPL_H_
