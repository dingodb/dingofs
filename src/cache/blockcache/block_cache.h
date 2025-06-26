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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_

#include "cache/blockcache/cache_store.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct PutOption {
  bool writeback{false};
  BlockContext block_ctx{BlockContext::kFromUnknown};
};

struct RangeOption {
  bool retrive{true};
  size_t block_size{0};
};

struct CacheOption {};

struct PrefetchOption {};

// async callback
using AsyncCallback = std::function<void(Status)>;

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  // block operations (sync)
  virtual Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
                     PutOption option = PutOption()) = 0;
  virtual Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer,
                       RangeOption option = RangeOption()) = 0;
  virtual Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                       CacheOption option = CacheOption()) = 0;
  virtual Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                          PrefetchOption option = PrefetchOption()) = 0;

  // block operations (async)
  virtual void AsyncPut(ContextSPtr ctx, const BlockKey& key,
                        const Block& block, AsyncCallback callback,
                        PutOption option = PutOption()) = 0;
  virtual void AsyncRange(ContextSPtr ctx, const BlockKey& key, off_t offset,
                          size_t length, IOBuffer* buffer,
                          AsyncCallback callback,
                          RangeOption option = RangeOption()) = 0;
  virtual void AsyncCache(ContextSPtr ctx, const BlockKey& key,
                          const Block& block, AsyncCallback callback,
                          CacheOption option = CacheOption()) = 0;
  virtual void AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                             size_t length, AsyncCallback callback,
                             PrefetchOption option = PrefetchOption()) = 0;

  // utility
  virtual bool HasCacheStore() const = 0;
  virtual bool EnableStage() const = 0;
  virtual bool EnableCache() const = 0;
  virtual bool IsCached(const BlockKey& key) const = 0;
};

using BlockCachePtr = BlockCache*;
using BlockCacheSPtr = std::shared_ptr<BlockCache>;
using BlockCacheUPtr = std::unique_ptr<BlockCache>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
