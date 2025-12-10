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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_

#include "cache/blockcache/block_cache.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/remotecache/segment_fetcher.h"
#include "cache/storage/storage.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

class RemoteBlockCacheImpl final : public BlockCache {
 public:
  explicit RemoteBlockCacheImpl(StorageSPtr storage);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
             PutOption option) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, RangeOption option) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
               CacheOption option) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                  PrefetchOption option) override;

  void AsyncPut(ContextSPtr ctx, const BlockKey& key, const Block& block,
                AsyncCallback cb, PutOption option) override;
  void AsyncRange(ContextSPtr ctx, const BlockKey& key, off_t offset,
                  size_t length, IOBuffer* buffer, AsyncCallback cb,
                  RangeOption option) override;
  void AsyncCache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                  AsyncCallback cb, CacheOption option) override;
  void AsyncPrefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                     AsyncCallback cb, PrefetchOption option) override;

  bool HasCacheStore() const override;
  bool EnableStage() const override;
  bool EnableCache() const override;
  bool IsCached(const BlockKey& key) const override;

 private:
  BlockCachePtr GetSelfPtr() { return this; }

  std::atomic<bool> running_;
  RemoteCacheNodeSPtr remote_node_;
  StorageSPtr storage_;
  BthreadJoinerUPtr joiner_;
  CacheRetrieverUPtr retriever_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_
