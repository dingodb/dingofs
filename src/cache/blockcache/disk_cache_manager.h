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

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/lru_cache.h"
#include "cache/blockcache/lru_common.h"
#include "cache/common/type.h"
#include "metrics/cache/blockcache/disk_cache_metric.h"
#include "utils/message_queue.h"

namespace dingofs {
namespace cache {

// phase: staging -> uploaded -> cached
enum class BlockPhase : uint8_t {
  kStaging = 0,
  kUploaded = 1,
  kCached = 2,
};

// Manage cache items and its capacity
class DiskCacheManager {
 public:
  DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout,
                   DiskCacheMetricSPtr metric);
  virtual ~DiskCacheManager() = default;

  virtual void Start();
  virtual void Shutdown();

  virtual void Add(const CacheKey& key, const CacheValue& value,
                   BlockPhase phase);
  virtual void Delete(const CacheKey& key);
  virtual bool Exist(const CacheKey& key);

  virtual bool StageFull() const;
  virtual bool CacheFull() const;

 private:
  enum class DeletionReason : uint8_t {
    kCacheFull,
    kCacheExpired,
  };

  using MessageType = std::pair<CacheItems, DeletionReason>;
  using MessageQueueType = utils::MessageQueue<MessageType>;
  using MessageQueueUPtr = std::unique_ptr<MessageQueueType>;

  void CheckFreeSpace();
  void CleanupFull(uint64_t goal_bytes, uint64_t goal_files);
  void CleanupExpire();
  void DeleteBlocks(const CacheItems& to_del, DeletionReason reason);
  void UpdateUsage(int64_t n, int64_t used_bytes);

  std::string GetRootDir() const;
  std::string GetCachePath(const CacheKey& key) const;
  std::string ToString(DeletionReason reason) const;

  std::atomic<bool> running_;
  BthreadMutex mutex_;
  uint64_t used_bytes_;
  const uint64_t capacity_bytes_;
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  DiskCacheLayoutSPtr layout_;
  // Only store cached block key
  LRUCacheUPtr cached_blocks_;
  // Store stage block key which will not deleted by anyone util it uploaded to
  // storage. It will causes io error if we delete the stage block which not
  // uploaded for we can't get block both local disk and remote storage.
  std::unordered_map<std::string, CacheValue> staging_blocks_;
  MessageQueueUPtr mq_;
  TaskThreadPoolUPtr thread_pool_;
  DiskCacheMetricSPtr metric_;
};

using DiskCacheManagerSPtr = std::shared_ptr<DiskCacheManager>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_MANAGER_H_
