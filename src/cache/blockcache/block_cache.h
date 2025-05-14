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

#include <atomic>
#include <memory>

#include "cache/blockcache/block_cache_metric.h"
#include "cache/blockcache/block_cache_throttle.h"
#include "cache/blockcache/block_cache_uploader.h"
#include "cache/blockcache/block_prefetcher.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/countdown.h"
#include "cache/common/common.h"
#include "cache/storage/buffer.h"
#include "cache/storage/storage.h"
#include "dataaccess/accesser.h"

namespace dingofs {
namespace cache {
namespace blockcache {

enum class StoreType : uint8_t {
  kNone = 0,
  kDisk = 1,
};

class BlockCache {
 public:
  struct PutOption {
    PutOption() : writeback(false) {}
    PutOption(BlockContext ctx) : ctx(ctx), writeback(false) {}

    BlockContext ctx;
    bool writeback;
  };

  struct RangeOption {
    RangeOption() : retive(true){};
    RangeOption(bool retive) : retive(retive){};

    bool retive;
  };

  struct CacheOption {
    CacheOption() = default;
  };

  struct FlushOption {
    FlushOption() = default;
  };

  virtual ~BlockCache() = default;

  virtual Status Init() = 0;

  virtual Status Shutdown() = 0;

  virtual Status Put(PutOption option, const BlockKey& key,
                     const Block& block) = 0;

  virtual Status Range(RangeOption option, const BlockKey& key, off_t offset,
                       size_t length, storage::IOBuffer* buffer) = 0;

  virtual Status Cache(CacheOption option, const BlockKey& key,
                       const Block& block) = 0;

  virtual Status Flush(FlushOption option, uint64_t ino) = 0;

  virtual void RunInBthread(std::function<void(BlockCache*)>);

  virtual void SubmitPrefetch(const BlockKey& key, size_t length) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual StoreType GetStoreType() = 0;
};

class BlockCacheImpl : public BlockCache {
 public:
  explicit BlockCacheImpl(BlockCacheOption option,
                          dataaccess::DataAccesserPtr data_accesser);

  ~BlockCacheImpl() override = default;

  Status Init() override;

  Status Shutdown() override;

  Status Put(PutOption option, const BlockKey& key,
             const Block& block) override;

  Status Range(RangeOption option, const BlockKey& key, off_t offset,
               size_t length, storage::IOBuffer* buffer) override;

  Status Cache(CacheOption option, const BlockKey& key,
               const Block& block) override;

  Status Flush(FlushOption option, uint64_t ino) override;

  void RunInBthread(std::function<void(BlockCache*)>) override;

  void SubmitPrefetch(const BlockKey& key, size_t length) override;

  bool IsCached(const BlockKey& key) override;

  StoreType GetStoreType() override;

 private:
  struct FuncArg {
    FuncArg(BlockCache* block_cache, std::function<void(BlockCache*)> func)
        : block_cache(block_cache), func(func) {}

    BlockCache* block_cache;
    std::function<void(BlockCache*)> func;
  };

  Status DoPrefetch(const BlockKey& key, size_t length);

  friend class BlockCacheBuilder;

  BlockCacheOption option_;
  std::atomic<bool> running_;
  std::shared_ptr<storage::Storage> storage_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<Countdown> stage_count_;
  std::shared_ptr<BlockCacheThrottle> throttle_;
  std::shared_ptr<BlockCacheUploader> uploader_;
  std::unique_ptr<BlockPrefetcher> prefetcher_;
  std::unique_ptr<BlockCacheMetric> metric_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
