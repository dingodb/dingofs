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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "client/blockcache/block_cache_metric.h"
#include "client/blockcache/block_cache_throttle.h"
#include "client/blockcache/block_cache_uploader.h"
#include "client/blockcache/block_prefetcher.h"
#include "client/blockcache/cache_store.h"
#include "client/blockcache/countdown.h"
#include "client/blockcache/error.h"
#include "client/blockcache/s3_client.h"
#include "client/common/config.h"

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::client::common::BlockCacheOption;
using ::dingofs::utils::TaskThreadPool;

enum class StoreType : uint8_t {
  NONE,
  DISK,
};

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual BCACHE_ERROR Init() = 0;

  virtual BCACHE_ERROR Shutdown() = 0;

  virtual BCACHE_ERROR Put(const BlockKey& key, const Block& block,
                           BlockContext ctx) = 0;

  virtual BCACHE_ERROR Range(const BlockKey& key, off_t offset, size_t length,
                             char* buffer, bool retrive = true) = 0;

  virtual BCACHE_ERROR PreFetch(const BlockKey& key, size_t length) = 0;

  virtual BCACHE_ERROR Cache(const BlockKey& key, const Block& block) = 0;

  virtual BCACHE_ERROR Flush(uint64_t ino) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual StoreType GetStoreType() = 0;
};

class BlockCacheImpl : public BlockCache {
 public:
  explicit BlockCacheImpl(BlockCacheOption option);

  ~BlockCacheImpl() override = default;

  BCACHE_ERROR Init() override;

  BCACHE_ERROR Shutdown() override;

  BCACHE_ERROR Put(const BlockKey& key, const Block& block,
                   BlockContext ctx) override;

  BCACHE_ERROR Range(const BlockKey& key, off_t offset, size_t length,
                     char* buffer, bool retrive = true) override;

  BCACHE_ERROR PreFetch(const BlockKey& key, size_t size) override;

  BCACHE_ERROR Cache(const BlockKey& key, const Block& block) override;

  BCACHE_ERROR Flush(uint64_t ino) override;

  bool IsCached(const BlockKey& key) override;

  StoreType GetStoreType() override;

 private:
  BCACHE_ERROR DoPreFetch(const BlockKey& key, size_t size);

 private:
  friend class BlockCacheBuilder;

 private:
  BlockCacheOption option_;
  std::atomic<bool> running_;
  std::shared_ptr<S3Client> s3_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<Countdown> stage_count_;
  std::shared_ptr<BlockCacheThrottle> throttle_;
  std::shared_ptr<BlockCacheUploader> uploader_;
  std::unique_ptr<BlockPrefetcher> prefetcher_;
  std::unique_ptr<BlockCacheMetric> metric_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_H_
