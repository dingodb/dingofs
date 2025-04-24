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
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_MANAGER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>

#include "base/cache/cache.h"
#include "base/queue/message_queue.h"
#include "base/time/time.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_metric.h"
#include "cache/blockcache/lru_cache.h"
#include "cache/common/local_filesystem.h"
#include "utils/concurrent/concurrent.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using base::queue::MessageQueue;
using cache::blockcache::LRUCache;
using utils::Mutex;
using utils::TaskThreadPool;

// phase: staging -> uploaded -> cached
enum class BlockPhase : uint8_t {
  kStaging = 0,
  kUploaded = 1,
  kCached = 2,
};

// Manage cache items and its capacity
class DiskCacheManager {
  enum class DeleteFrom : uint8_t {
    kCacheFull,
    kCacheExpired,
  };

  using MessageType = std::pair<CacheItems, DeleteFrom>;
  using MessageQueueType = MessageQueue<MessageType>;

 public:
  DiskCacheManager(uint64_t capacity, std::shared_ptr<DiskCacheLayout> layout,
                   std::shared_ptr<LocalFileSystem> fs,
                   std::shared_ptr<DiskCacheMetric> metric);

  virtual ~DiskCacheManager() = default;

  virtual void Start();

  virtual void Stop();

  virtual void Add(const BlockKey& key, const CacheValue& value,
                   BlockPhase phase);

  virtual void Delete(const BlockKey& key);

  virtual bool Exist(const BlockKey& key);

  virtual bool StageFull() const;

  virtual bool CacheFull() const;

 private:
  void CheckFreeSpace();

  void CleanupFull(uint64_t goal_bytes, uint64_t goal_files);

  void CleanupExpire();

  void DeleteBlocks(const CacheItems& to_del, DeleteFrom);

  void UpdateUsage(int64_t n, int64_t bytes);

  std::string GetCachePath(const CacheKey& key);

  static std::string StrFrom(DeleteFrom from);

 private:
  Mutex mutex_;
  uint64_t used_bytes_;
  uint64_t capacity_;
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  std::atomic<bool> running_;
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::unique_ptr<LRUCache> lru_;                        // store cache block
  std::unordered_map<std::string, CacheValue> staging_;  // store stage block
  std::unique_ptr<MessageQueueType> mq_;
  std::shared_ptr<DiskCacheMetric> metric_;
  std::unique_ptr<TaskThreadPool<>> task_pool_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_MANAGER_H_
