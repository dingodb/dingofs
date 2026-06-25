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

#ifndef DINGOFS_SRC_CACHE_LOCAL_DISK_CACHE_MANAGER_H_
#define DINGOFS_SRC_CACHE_LOCAL_DISK_CACHE_MANAGER_H_

#include <absl/strings/str_format.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>

#include <array>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <utility>

#include "cache/local/disk_cache_layout.h"
#include "cache/local/lru_cache.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {

struct DiskCacheManagerMetrics {
  DiskCacheManagerMetrics(uint64_t cache_index)
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

using DiskCacheManagerMetricsUPtr = std::unique_ptr<DiskCacheManagerMetrics>;

enum class CacheEntryState : uint8_t {
  // The only local copy of a not-yet-uploaded writeback block. It is readable
  // through the hard-linked cache path, but must never be evicted or deleted by
  // normal cache cleanup.
  kStaging = 0,
  // A normal read cache block. It can be evicted by LRU/expiry cleanup.
  kCached = 1,
};

struct CacheEntry {
  CacheEntry() = default;
  CacheEntry(CacheKey key, CacheValue value, CacheEntryState state)
      : key(std::move(key)), value(value), state(state) {}

  CacheKey key;
  CacheValue value;
  CacheEntryState state{CacheEntryState::kCached};
};

// Manage cache items and its capacity.
//
// The cache index (entry table + cached-block LRU + capacity accounting) is
// sharded by block-key hash into kShardCount independent shards, each with its
// own mutex. Operations on different keys land on different shards and no
// longer serialize on a single global lock. This mirrors the MemCache sharding
// paradigm (src/cache/local/mem_cache.h).
class DiskCacheManager {
 public:
  static constexpr size_t kShardCount =
      32;  // aligned with MemCache, power of 2
  static_assert((kShardCount & (kShardCount - 1)) == 0,
                "kShardCount must be a power of 2");

  // Same as MemCache::ShardIndex: low bits of the filename hash. Staging and
  // cached states of one block share key.Filename(), so state transitions
  // always land on the same shard.
  static size_t ShardIndex(const std::string& filename) {
    return std::hash<std::string>{}(filename) & (kShardCount - 1);
  }

  DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout);
  virtual ~DiskCacheManager() = default;

  virtual void Start();
  virtual void Shutdown();

  virtual void AddStaging(const CacheKey& key, const CacheValue& value);
  virtual void PromoteStagingToCached(const CacheKey& key);
  virtual void AddCached(const CacheKey& key, const CacheValue& value);
  virtual void DeleteCached(const CacheKey& key);
  virtual bool Exist(const CacheKey& key);

  virtual bool StageFull() const;
  virtual bool CacheFull() const;

 private:
  struct ToDel {
    CacheItems items;
    std::string reason;
  };

  // Cache-line aligned to avoid false sharing between adjacent shards
  // (mirrors mem_cache.h Shard). Every block is tracked in entries; only normal
  // cached blocks are linked into cached_lru and can be selected for eviction.
  struct alignas(64) Shard {
    mutable bthread::Mutex mutex;
    uint64_t used_bytes{0};  // used bytes of this shard (staging + cached)
    std::unordered_map<std::string, CacheEntry> entries;
    LRUCacheUPtr cached_lru;
  };

  Shard& GetShard(const std::string& filename) {
    return shards_[ShardIndex(filename)];
  }

  void Init();

  void CheckFreeSpace();
  // Evict from a single shard's LRU; assumes shard.mutex is held.
  void CleanupFullIfNeededLocked(Shard& shard);
  void CleanupFullLocked(Shard& shard, uint64_t want_free_bytes,
                         uint64_t want_free_files);
  // Spread a whole-disk want_free across all shards, locking each in turn.
  void CleanupAllShardsFull(uint64_t want_free_bytes, uint64_t want_free_files);
  void CleanupExpire();
  static int HandleTask(void* meta, bthread::TaskIterator<ToDel>& iter);
  void DeleteBlocks(const ToDel& to_del);
  void RemoveEntryLocked(Shard& shard, const std::string& filename);
  // Update shard.used_bytes (locked) and total_used_bytes_ (atomic) + bvar.
  // Assumes shard.mutex is held.
  void UpdateUsageLocked(Shard& shard, int64_t n, int64_t used_bytes);

  std::string GetRootDir() const;
  std::string GetCachePath(const CacheKey& key) const;

  std::atomic<bool> running_;
  const uint64_t capacity_bytes_;        // whole-disk capacity
  const uint64_t shard_capacity_bytes_;  // ceil(capacity_bytes_ / kShardCount)
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  // whole-disk used bytes, only for monitoring; per-shard eviction reads its
  // own shard.used_bytes instead, so it never contends on this atomic.
  std::atomic<int64_t> total_used_bytes_;
  std::array<Shard, kShardCount> shards_;
  utils::TaskThreadPoolUPtr thread_pool_;
  DiskCacheLayoutSPtr layout_;
  bthread::ExecutionQueueId<ToDel> queue_id_;
  DiskCacheManagerMetricsUPtr vars_;
};

using DiskCacheManagerSPtr = std::shared_ptr<DiskCacheManager>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_DISK_CACHE_MANAGER_H_
