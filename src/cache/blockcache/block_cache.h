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
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "cache/blockcache/block_cache_metric.h"
#include "cache/blockcache/block_cache_throttle.h"
#include "cache/blockcache/block_cache_uploader.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/countdown.h"
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/common/io_buffer.h"
#include "cache/common/s3_client.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using cache::common::BlockCacheOptions;
using cache::common::Errno;
using cache::common::IOBuffer;
using utils::TaskThreadPool;

enum class StoreType : uint8_t {
  NONE,
  DISK,
};

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Errno Init() = 0;

  virtual Errno Shutdown() = 0;

  virtual Errno Put(const BlockKey& key, const Block& block,
                    BlockContext ctx) = 0;

  virtual Errno Range(const BlockKey& key, off_t offset, size_t length,
                      IOBuffer* buffer, bool retrive = true) = 0;

  virtual Errno Cache(const BlockKey& key, const Block& block) = 0;

  virtual Errno Flush(uint64_t ino) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual StoreType GetStoreType() = 0;
};

class BlockCacheImpl : public BlockCache {
 public:
  explicit BlockCacheImpl(BlockCacheOptions options);

  ~BlockCacheImpl() override = default;

  Errno Init() override;

  Errno Shutdown() override;

  Errno Put(const BlockKey& key, const Block& block, BlockContext ctx) override;

  Errno Range(const BlockKey& key, off_t offset, size_t length,
              IOBuffer* buffer, bool retrive = true) override;

  Errno Cache(const BlockKey& key, const Block& block) override;

  Errno Flush(uint64_t ino) override;

  bool IsCached(const BlockKey& key) override;

  StoreType GetStoreType() override;

 private:
  friend class BlockCacheBuilder;

 private:
  BlockCacheOptions options_;
  std::atomic<bool> running_;
  std::shared_ptr<S3Client> s3_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<Countdown> stage_count_;
  std::shared_ptr<BlockCacheThrottle> throttle_;
  std::shared_ptr<BlockCacheUploader> uploader_;
  std::unique_ptr<BlockCacheMetric> metric_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
