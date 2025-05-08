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
#include "dataaccess/block_accesser.h"

namespace dingofs {
namespace cache {
namespace blockcache {

enum class StoreType : uint8_t {
  kNone = 0,
  kDisk = 1,
};

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Status Init() = 0;

  virtual Status Shutdown() = 0;

  virtual Status Put(const BlockKey& key, const Block& block,
                     BlockContext ctx) = 0;

  virtual Status Range(const BlockKey& key, off_t offset, size_t length,
                       char* buffer, bool retrive = true) = 0;

  virtual Status Cache(const BlockKey& key, const Block& block) = 0;

  virtual Status Flush(uint64_t ino) = 0;

  virtual void SubmitPrefetch(const BlockKey& key, size_t length) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual StoreType GetStoreType() = 0;
};

class BlockCacheImpl : public BlockCache {
 public:
  explicit BlockCacheImpl(BlockCacheOption option,
                          dataaccess::BlockAccesser* block_accesser);

  ~BlockCacheImpl() override = default;

  Status Init() override;

  Status Shutdown() override;

  Status Put(const BlockKey& key, const Block& block,
             BlockContext ctx) override;

  Status Range(const BlockKey& key, off_t offset, size_t length, char* buffer,
               bool retrive = true) override;

  Status Cache(const BlockKey& key, const Block& block) override;

  Status Flush(uint64_t ino) override;

  void SubmitPrefetch(const BlockKey& key, size_t length) override;

  bool IsCached(const BlockKey& key) override;

  StoreType GetStoreType() override;

 private:
  Status DoPrefetch(const BlockKey& key, size_t length);

 private:
  friend class BlockCacheBuilder;

 private:
  BlockCacheOption option_;
  std::atomic<bool> running_;
  dataaccess::BlockAccesser* block_accesser_;
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
