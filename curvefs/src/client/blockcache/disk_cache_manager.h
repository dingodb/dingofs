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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>

#include "curvefs/src/base/cache/cache.h"
#include "curvefs/src/base/queue/message_queue.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/disk_cache_layout.h"
#include "curvefs/src/client/blockcache/disk_cache_metric.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"
#include "curvefs/src/client/blockcache/lru_cache.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::Mutex;
using ::curve::common::TaskThreadPool;
using ::curvefs::base::cache::Cache;
using ::curvefs::base::queue::MessageQueue;
using ::curvefs::base::time::TimeSpec;
using ::curvefs::client::blockcache::LRUCache;

// Manage cache items and its capacity
class DiskCacheManager {
  enum class DeleteFrom {
    CACHE_FULL,
    CACHE_EXPIRED,
  };

  using MessageType = std::pair<std::shared_ptr<CacheItems>, DeleteFrom>;
  using MessageQueueType = MessageQueue<MessageType>;

 public:
  DiskCacheManager(uint64_t capacity, std::shared_ptr<DiskCacheLayout> layout,
                   std::shared_ptr<LocalFileSystem> fs,
                   std::shared_ptr<DiskCacheMetric> metric);

  virtual ~DiskCacheManager() = default;

  virtual void Start();

  virtual void Stop();

  virtual void Add(const BlockKey& key, const CacheValue& value);

  virtual BCACHE_ERROR Get(const BlockKey& key, CacheValue* value);

  virtual bool StageFull() const;

  virtual bool CacheFull() const;

 private:
  void CheckFreeSpace();

  void CleanupFull(uint64_t goal_bytes, uint64_t goal_files);

  void CleanupExpire();

  void DeleteBlocks(const std::shared_ptr<CacheItems>& to_del, DeleteFrom);

  void AddUsedBytes(int64_t bytes);

  std::string GetCachePath(const CacheKey& key);

 private:
  Mutex mutex_;
  uint64_t used_bytes_;
  uint64_t capacity_;
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  std::atomic<bool> running_;
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::unique_ptr<LRUCache> lru_;
  std::unique_ptr<MessageQueueType> mq_;
  std::shared_ptr<DiskCacheMetric> metric_;
  std::unique_ptr<TaskThreadPool<>> task_pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_MANAGER_H_
