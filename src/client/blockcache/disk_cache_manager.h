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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_MANAGER_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>

#include "base/cache/cache.h"
#include "base/queue/message_queue.h"
#include "base/time/time.h"
#include "client/blockcache/cache_store.h"
#include "client/blockcache/disk_cache_layout.h"
#include "client/blockcache/disk_cache_metric.h"
#include "client/blockcache/local_filesystem.h"
#include "client/blockcache/lru_cache.h"
#include "utils/concurrent/concurrent.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::utils::Mutex;
using ::dingofs::utils::TaskThreadPool;
using ::dingofs::base::cache::Cache;
using ::dingofs::base::queue::MessageQueue;
using ::dingofs::base::time::TimeSpec;
using ::dingofs::client::blockcache::LRUCache;

// Manage cache items and its capacity
class DiskCacheManager {
  enum class DeleteFrom {
    CACHE_FULL,
    CACHE_EXPIRED,
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

  virtual void Add(const BlockKey& key, const CacheValue& value);

  virtual BCACHE_ERROR Get(const BlockKey& key, CacheValue* value);

  virtual void Delete(const BlockKey& key);

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
  std::unique_ptr<LRUCache> lru_;
  std::unique_ptr<MessageQueueType> mq_;
  std::shared_ptr<DiskCacheMetric> metric_;
  std::unique_ptr<TaskThreadPool<>> task_pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_MANAGER_H_
