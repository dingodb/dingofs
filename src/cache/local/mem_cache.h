/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-05-28
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_LOCAL_MEM_CACHE_H_
#define DINGOFS_SRC_CACHE_LOCAL_MEM_CACHE_H_

#include <absl/strings/str_format.h>
#include <bthread/mutex.h>
#include <bvar/bvar.h>

#include <array>
#include <atomic>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include "cache/local/cache_store.h"
#include "common/const.h"

namespace dingofs {
namespace cache {

struct MemCacheOption {
  uint64_t cache_size_mb{0};
};

struct MemCacheMetrics {
  explicit MemCacheMetrics(uint64_t cache_size_mb)
      : capacity(Name("capacity"), cache_size_mb * kMiB),
        running_status(Name("running_status"), "down"),
        used_bytes(Name("used_bytes")),
        cache_hits(Name("cache_hits")),
        cache_misses(Name("cache_misses")),
        stage_skips(Name("stage_skips")) {}

  static std::string Name(const std::string& name) {
    return absl::StrFormat("dingofs_mem_cache_%s", name);
  }

  void Reset() {
    running_status.set_value("down");
    used_bytes.reset();
    cache_hits.reset();
    cache_misses.reset();
    stage_skips.reset();
  }

  bvar::Status<int64_t> capacity;
  bvar::Status<std::string> running_status;
  bvar::Adder<int64_t> used_bytes;
  bvar::Adder<int64_t> cache_hits;
  bvar::Adder<int64_t> cache_misses;
  bvar::Adder<int64_t> stage_skips;
};

using MemCacheMetricsUPtr = std::unique_ptr<MemCacheMetrics>;

// Sharded in-memory CacheStore. Each shard owns an independent mutex, LRU
// list and index, so concurrent accesses on different keys do not serialize.
class MemCache final : public CacheStore {
 public:
  static constexpr size_t kShardCount = 32;
  static_assert((kShardCount & (kShardCount - 1)) == 0,
                "kShardCount must be a power of 2");

  static size_t ShardIndex(const std::string& key) {
    return std::hash<std::string>{}(key) & (kShardCount - 1);
  }

  explicit MemCache(MemCacheOption option);
  ~MemCache() override = default;

  Status Start(UploadFunc uploader) override;
  Status Shutdown() override;

  Status Stage(BlockHandle handle, IOBuffer block,
               StageOption option = {}) override;
  Status RemoveStage(BlockHandle handle,
                     RemoveStageOption option = {}) override;
  Status Cache(BlockHandle handle, IOBuffer block,
               CacheOption option = {}) override;
  Status Load(BlockHandle handle, off_t offset, size_t length, IOBuffer* buffer,
              LoadOption option = {}) override;

  std::string Id() const override { return uuid_; }

  bool IsRunning() const override {
    return running_.load(std::memory_order_relaxed);
  }

  bool IsCached(const BlockHandle& handle) const override;
  bool IsFull(const BlockHandle& handle) const override;
  bool Dump(Json::Value& value) const override;

 private:
  struct Entry {
    std::string key;
    IOBuffer data;
  };
  using EntryList = std::list<Entry>;

  // Cache-line aligned to avoid false sharing between adjacent shards.
  struct alignas(64) Shard {
    mutable bthread::Mutex mutex;
    uint64_t used_bytes{0};
    EntryList lru;  // front = most recently used
    std::unordered_map<std::string, EntryList::iterator> index;
  };

  Shard& GetShard(const std::string& key) {
    return shards_[ShardIndex(key)];
  }
  const Shard& GetShard(const std::string& key) const {
    return shards_[ShardIndex(key)];
  }

  // Inserts or updates under `shard.mutex`. Evicts LRU tail as needed.
  void Insert(Shard& shard, const std::string& key, IOBuffer block);
  // Assumes `shard.mutex` is held.
  void EvictLocked(Shard& shard, uint64_t need_bytes);

  std::atomic<bool> running_;
  const MemCacheOption option_;
  const uint64_t shard_capacity_bytes_;
  const std::string uuid_;
  UploadFunc uploader_;

  std::array<Shard, kShardCount> shards_;

  MemCacheMetricsUPtr vars_;
};

using MemCacheSPtr = std::shared_ptr<MemCache>;
using MemCacheUPtr = std::unique_ptr<MemCache>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_MEM_CACHE_H_
