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
#include <cstdint>
#include <string>
#include <vector>

#include "cache/local/cache_entry.h"
#include "cache/local/cache_policy.h"
#include "cache/local/disk_cache_layout.h"
#include "common/block/block_handle.h"
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
        cache_full(Name("cache_full"), false),
        evict_blocks(Name("evict_blocks")),
        evict_bytes(Name("evict_bytes")),
        expire_blocks(Name("expire_blocks")),
        eviction_policy(Name("eviction_policy"), "") {}

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
    evict_blocks.reset();
    evict_bytes.reset();
    expire_blocks.reset();
  }

  std::string prefix;
  bvar::Status<int64_t> used_bytes;
  bvar::Adder<int64_t> stage_blocks;
  bvar::Status<bool> stage_full;
  bvar::Adder<int64_t> cache_blocks;
  bvar::Adder<int64_t> cache_bytes;
  bvar::Status<bool> cache_full;
  bvar::Adder<int64_t> evict_blocks;   // blocks freed by capacity/free-space
  bvar::Adder<int64_t> evict_bytes;    // bytes freed by capacity/free-space
  bvar::Adder<int64_t> expire_blocks;  // blocks freed by TTL expiry
  bvar::Status<std::string> eviction_policy;
};

using DiskCacheManagerMetricsUPtr = std::unique_ptr<DiskCacheManagerMetrics>;

// Manage cache items and its capacity.
//
// Each block owns a single CacheEntry living in one per-shard index
// (node_hash_map, keyed by BlockHandle). That entry is simultaneously the node
// linked into the shard's EvictionPolicy ordering, so a read hit is a single
// lookup plus one bit/counter write -- no second map, no string key, no
// allocation. Staging (not-yet-uploaded writeback) blocks are present in the
// index but never handed to the policy, so they can never be evicted.
//
// The index is sharded by BlockHandle::Hash() into kShardCount independent
// shards, each with its own mutex.
class DiskCacheManager {
 public:
  static constexpr size_t kShardCount = 32;  // power of 2
  static_assert((kShardCount & (kShardCount - 1)) == 0,
                "kShardCount must be a power of 2");

  static size_t ShardIndex(const BlockHandle& handle) {
    return handle.Hash() & (kShardCount - 1);
  }

  DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout,
                   std::string eviction_policy = "");
  virtual ~DiskCacheManager() = default;

  virtual void Start();
  virtual void Shutdown();

  virtual void AddStaging(const BlockHandle& handle, uint32_t size,
                          uint32_t atime_sec);
  virtual void PromoteStagingToCached(const BlockHandle& handle);
  virtual void AddCached(const BlockHandle& handle, uint32_t size,
                         uint32_t atime_sec);
  virtual void DeleteCached(const BlockHandle& handle);
  virtual bool Exist(const BlockHandle& handle);

  virtual bool StageFull() const;
  virtual bool CacheFull() const;

 private:
  struct DelItem {
    BlockHandle handle;
    uint32_t size;
  };
  struct ToDel {
    std::vector<DelItem> items;
    std::string reason;
  };

  // Cache-line aligned to avoid false sharing between adjacent shards. The
  // index owns every block's CacheEntry; only cached (non-staged) entries are
  // linked into the policy and can be evicted.
  struct alignas(64) Shard {
    mutable bthread::Mutex mutex;
    uint64_t used_bytes{0};  // staging + cached bytes of this shard
    CacheIndex index;
    EvictionPolicyUPtr policy;
  };

  Shard& GetShard(const BlockHandle& handle) {
    return shards_[ShardIndex(handle)];
  }

  void Init();

  void CheckFreeSpace();
  // Evict from a single shard when it reaches capacity; assumes mutex held.
  void CleanupFullIfNeededLocked(Shard& shard);
  void CleanupFullLocked(Shard& shard, uint64_t want_free_bytes,
                         uint64_t want_free_files);
  void CleanupAllShardsFull(uint64_t want_free_bytes, uint64_t want_free_files);
  void CleanupExpire();
  // Account for, erase from the index, and async-delete the given victims;
  // returns total bytes freed. Assumes shard.mutex is held.
  uint64_t FlushVictimsLocked(Shard& shard, const CacheVictims& victims,
                              const std::string& reason);
  static int HandleTask(void* meta, bthread::TaskIterator<ToDel>& iter);
  void DeleteBlocks(const ToDel& to_del);
  // Update shard.used_bytes and total_used_bytes_ + bvar. Assumes mutex held.
  void UpdateUsageLocked(Shard& shard, int64_t n, int64_t used_bytes);

  std::string GetRootDir() const;
  std::string GetCachePath(const BlockHandle& handle) const;

  std::atomic<bool> running_;
  const uint64_t capacity_bytes_;        // whole-disk capacity
  const uint64_t shard_capacity_bytes_;  // ceil(capacity_bytes_ / kShardCount)
  const std::string eviction_policy_;    // resolved policy name
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  // Coarse, seconds-granularity clock, refreshed by the free-space loop and
  // read on the hit path so Exist never issues a clock syscall.
  std::atomic<uint32_t> now_sec_;
  // whole-disk used bytes, monitoring only; eviction reads its own
  // shard.used_bytes, so it never contends on this atomic.
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
