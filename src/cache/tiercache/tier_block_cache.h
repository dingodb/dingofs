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
 * Created Date: 2025-05-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_TIERCACHE_TIER_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_TIERCACHE_TIER_BLOCK_CACHE_H_

#include <memory>

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/config/config.h"
#include "cache/storage/storage.h"

namespace dingofs {
namespace cache {

class TierBlockCache final : public BlockCache {
 public:
  TierBlockCache(BlockCacheOption local_cache_option,
                 RemoteBlockCacheOption remote_cache_option,
                 StorageSPtr storage);

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
  BlockCacheSPtr GetSelfSPtr() { return shared_from_this(); }

  std::atomic<bool> running_;
  StorageSPtr storage_;
  BlockCacheUPtr local_block_cache_;
  BlockCacheUPtr remote_block_cache_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TIERCACHE_TIER_BLOCK_CACHE_H_
