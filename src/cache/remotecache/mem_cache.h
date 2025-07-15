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
 * Created Date: 2025-07-15
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_MEM_CACHE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_MEM_CACHE_H_

#include <bthread/execution_queue.h>

#include <memory>
#include <unordered_map>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/lru_common.h"
#include "cache/common/type.h"
#include "common/status.h"
#include "utils/lru_cache.h"

namespace dingofs {
namespace cache {

class MemCache {
 public:
  virtual ~MemCache() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual void Put(const BlockKey& key, const Block& block) = 0;
  virtual Status Get(const BlockKey& key, Block* block) = 0;
  virtual bool Exist(const BlockKey& key) = 0;
};

using MemCacheSPtr = std::shared_ptr<MemCache>;

class MemCacheImpl final : public MemCache {
 public:
  explicit MemCacheImpl(size_t capacity);

  Status Start() override;
  Status Shutdown() override;

  void Put(const BlockKey& key, const Block& block) override;
  Status Get(const BlockKey& key, Block* block) override;
  bool Exist(const BlockKey& key) override;

 private:
  void UpdateUsage(size_t add_bytes);
  void CleanupFull(size_t goal_bytes);
  static int HandleEvict(void* meta,
                         bthread::TaskIterator<std::vector<std::string>>& iter);

  BthreadMutex mutex_;  // for used_, capacity_
  std::atomic<bool> running_;
  size_t used_;
  size_t capacity_;
  std::unique_ptr<utils::LRUCache<std::string, Block>> lru_;
};

};  // namespace cache
};  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_MEM_CACHE_H_
