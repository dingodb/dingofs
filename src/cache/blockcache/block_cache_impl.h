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

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_uploader.h"
#include "cache/common/context.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

class BlockCacheImpl final : public BlockCache {
 public:
  explicit BlockCacheImpl(StorageClient* storage_client);
  explicit BlockCacheImpl(StorageClientPoolSPtr storage_client_pool);
  ~BlockCacheImpl() override;

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

  void AsyncPut(ContextSPtr ctx, const BlockKey& key, const Block& block,
                AsyncCallback cb, PutOption option = PutOption()) override;
  void AsyncRange(ContextSPtr ctx, const BlockKey& key, off_t offset,
                  size_t length, IOBuffer* buffer, AsyncCallback cb,
                  RangeOption option = RangeOption()) override;
  void AsyncCache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                  AsyncCallback cb,
                  CacheOption option = CacheOption()) override;
  void AsyncPrefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                     AsyncCallback cb,
                     PrefetchOption option = PrefetchOption()) override;

  bool IsEnabled() const override { return FLAGS_cache_store != "none"; }

  bool EnableStage() const override {
    return IsEnabled() && FLAGS_enable_stage;
  }

  bool EnableCache() const override {
    return IsEnabled() && FLAGS_enable_cache;
  }

  bool IsCached(const BlockKey& key) const override {
    return store_->IsCached(key);
  }

 private:
  friend class BlockCacheBuilder;

  BlockCache* GetSelfPtr() { return this; }

  Status StoragePut(ContextSPtr ctx, const BlockKey& key, const Block& block);
  Status StorageRange(ContextSPtr ctx, const BlockKey& key, off_t offset,
                      size_t length, IOBuffer* buffer);

  std::atomic<bool> running_;
  StorageClientPoolSPtr storage_client_pool_;
  CacheStoreSPtr store_;
  BlockCacheUploaderSPtr uploader_;
  iutil::BthreadJoinerUPtr joiner_;
  iutil::InflightTrackerSPtr cache_tracker_;
  iutil::InflightTrackerSPtr prefetch_tracker_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_IMPL_H_
