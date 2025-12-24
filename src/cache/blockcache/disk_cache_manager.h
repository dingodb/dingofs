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

#include <bthread/execution_queue.h>
#include <bthread/mutex.h>

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/lru_cache.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {

struct DiskCacheManagerVarsCollector {
  DiskCacheManagerVarsCollector(uint64_t cache_index)
      : prefix(absl::StrFormat("dingofs_disk_cache_%d", cache_index)),
        used_bytes(Name("used_bytes"), 0),
        stage_blocks(Name("stage_blocks")),
        stage_full(Name("stage_full"), false),
        cache_blocks(Name("cache_blocks")),
        cache_bytes(Name("cache_bytes")),
        cache_full(Name("cache_full"), false) {}

  std::string Name(const std::string& name) const {
    CHECK_GT(prefix.length(), 0);
    return absl::StrFormat("%s_%s", prefix, name);
  }

  void Reset() {
    used_bytes.set_value(0);
    stage_blocks.reset();
    stage_full.set_value(false);
    cache_blocks.reset();
    cache_bytes.reset();
    cache_full.set_value(false);
  }

  std::string prefix;
  bvar::Status<int64_t> used_bytes;
  bvar::Adder<int64_t> stage_blocks;
  bvar::Status<bool> stage_full;
  bvar::Adder<int64_t> cache_blocks;
  bvar::Adder<int64_t> cache_bytes;
  bvar::Status<bool> cache_full;
};

using DiskCacheManagerVarsCollectorUPtr =
    std::unique_ptr<DiskCacheManagerVarsCollector>;

// phase: staging -> uploaded -> cached
enum class BlockPhase : uint8_t {
  kStaging = 0,
  kUploaded = 1,
  kCached = 2,
};

// Manage cache items and its capacity
class DiskCacheManager {
 public:
  DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout);
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
  struct ToDel {
    CacheItems items;
    std::string reason;
  };

  void Init();

  void CheckFreeSpace();
  void CleanupFull(uint64_t want_free_bytes, uint64_t want_free_files);
  void CleanupExpire();
  static int HandleTask(void* meta, bthread::TaskIterator<ToDel>& iter);
  void DeleteBlocks(const ToDel& to_del);
  void UpdateUsage(int64_t n, int64_t used_bytes);

  std::string GetRootDir() const;
  std::string GetCachePath(const CacheKey& key) const;

  // For cache_blocks_ and staging_blocks_:
  // (1) cache_blocks_: only store cached block key
  // (2) staging_blocks_: store stage block key which will not deleted by anyone
  // util it uploaded to storage. It will causes io error if we delete the stage
  // block which not uploaded for we can't get block both local disk and remote
  // storage.
  std::atomic<bool> running_;
  bthread::Mutex mutex_;
  uint64_t used_bytes_;
  const uint64_t capacity_bytes_;
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  LRUCacheUPtr cached_blocks_;
  std::unordered_map<std::string, CacheValue> staging_blocks_;
  utils::TaskThreadPoolUPtr thread_pool_;
  DiskCacheLayoutSPtr layout_;
  bthread::ExecutionQueueId<ToDel> queue_id_;
  DiskCacheManagerVarsCollectorUPtr vars_;
};

using DiskCacheManagerSPtr = std::shared_ptr<DiskCacheManager>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_MANAGER_H_
