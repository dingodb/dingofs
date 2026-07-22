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

#include "cache/blockcache/admission.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/eviction_guard.h"
#include "cache/blockcache/eviction_policy.h"
#include "cache/blockcache/free_space_monitor.h"
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
        cache_full(Name("cache_full"), false),
        free_space_band(Name("free_space_band"), 0),
        evict_blocks_capacity(Name("evict_blocks_capacity")),
        evict_bytes_capacity(Name("evict_bytes_capacity")),
        evict_blocks_free_space(Name("evict_blocks_free_space")),
        evict_bytes_free_space(Name("evict_bytes_free_space")),
        evict_blocks_expire(Name("evict_blocks_expire")),
        evict_bytes_expire(Name("evict_bytes_expire")),
        eviction_policy(Name("eviction_policy"), ""),
        s3fifo_small_bytes(Name("s3fifo_small_bytes"), 0),
        ghost_entries(Name("ghost_entries"), 0),
        ghost_hits(Name("ghost_hits"), 0),
        protected_bytes(Name("protected_bytes"), 0),
        protect_skips(Name("protect_skips"), 0),
        evict_force_unprotect(Name("evict_force_unprotect"), 0),
        admit_accepts(Name("admit_accepts"), 0),
        admit_rejects_second_hit(Name("admit_rejects_second_hit"), 0),
        admit_rejects_write_budget(Name("admit_rejects_write_budget"), 0) {}

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
    free_space_band.set_value(0);
    evict_blocks_capacity.reset();
    evict_bytes_capacity.reset();
    evict_blocks_free_space.reset();
    evict_bytes_free_space.reset();
    evict_blocks_expire.reset();
    evict_bytes_expire.reset();
    s3fifo_small_bytes.set_value(0);
    ghost_entries.set_value(0);
    ghost_hits.set_value(0);
    protected_bytes.set_value(0);
    protect_skips.set_value(0);
    evict_force_unprotect.set_value(0);
    admit_accepts.set_value(0);
    admit_rejects_second_hit.set_value(0);
    admit_rejects_write_budget.set_value(0);
  }

  std::string prefix;
  bvar::Status<int64_t> used_bytes;
  bvar::Adder<int64_t> stage_blocks;
  bvar::Status<bool> stage_full;
  bvar::Adder<int64_t> cache_blocks;
  bvar::Adder<int64_t> cache_bytes;
  bvar::Status<bool> cache_full;
  bvar::Status<int64_t> free_space_band;
  bvar::Adder<int64_t> evict_blocks_capacity;
  bvar::Adder<int64_t> evict_bytes_capacity;
  bvar::Adder<int64_t> evict_blocks_free_space;
  bvar::Adder<int64_t> evict_bytes_free_space;
  bvar::Adder<int64_t> evict_blocks_expire;
  bvar::Adder<int64_t> evict_bytes_expire;
  bvar::Status<std::string> eviction_policy;
  bvar::Status<int64_t> s3fifo_small_bytes;
  bvar::Status<int64_t> ghost_entries;
  bvar::Status<int64_t> ghost_hits;
  bvar::Status<int64_t> protected_bytes;
  bvar::Status<int64_t> protect_skips;
  bvar::Status<int64_t> evict_force_unprotect;
  bvar::Status<int64_t> admit_accepts;
  bvar::Status<int64_t> admit_rejects_second_hit;
  bvar::Status<int64_t> admit_rejects_write_budget;
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
  enum class EvictReason : uint8_t {
    kCapacity = 0,
    kFreeSpace = 1,
    kExpire = 2,
  };

  DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout);
  virtual ~DiskCacheManager() = default;

  virtual void Start();
  virtual void Shutdown();

  // admission decision for a cache-fill disk write; warmup always admits,
  // stage/writeback does not route through here
  virtual bool Admit(const CacheKey& key, BlockSource source, size_t size);
  virtual void Add(const CacheKey& key, const CacheValue& value,
                   BlockPhase phase);
  virtual void Delete(const CacheKey& key);
  // the only operation counted as an access: refreshes recency on a real hit
  virtual void Touch(const CacheKey& key);
  // pure query, never promotes: probing must not fake hotness
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
  void CleanupFull(uint64_t want_free_bytes, uint64_t want_free_files,
                   EvictReason reason);
  void CleanupExpire();
  static int HandleTask(void* meta, bthread::TaskIterator<ToDel>& iter);
  // Enqueues blocks for async unlink and tracks them as in-flight frees
  // until DeleteBlocks() completes them.
  void SubmitDelete(CacheItems items, EvictReason reason);
  void DeleteBlocks(const ToDel& to_del);
  void RecordEvicted(EvictReason reason, uint64_t blocks, uint64_t bytes);
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
  // deletions enqueued for async unlink, not yet visible to statfs
  std::atomic<uint64_t> inflight_free_bytes_;
  std::atomic<uint64_t> inflight_free_files_;
  EvictionPolicyUPtr cached_blocks_;
  EvictionGuard guard_;
  AdmissionController admission_;
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
