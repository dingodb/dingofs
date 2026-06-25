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

#include "cache/local/disk_cache_manager.h"

#include <brpc/reloadable_flags.h>
#include <bthread/mutex.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "cache/common/macro.h"
#include "cache/iutil/file_util.h"
#include "common/const.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {

DEFINE_double(free_space_ratio, 0.1,
              "minimum free-space ratio before local cache cleanup is "
              "triggered");

DEFINE_uint32(cache_expire_s, 259200,
              "expiration time for cached blocks in seconds; staged blocks are "
              "not expired");
DEFINE_validator(cache_expire_s, brpc::PassValidate);

DEFINE_uint32(cache_cleanup_expire_interval_ms, 1000,
              "interval for scanning and removing expired cached blocks in "
              "milliseconds");
DEFINE_validator(cache_cleanup_expire_interval_ms, brpc::PassValidate);

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   DiskCacheLayoutSPtr layout)
    : running_(false),
      capacity_bytes_(capacity),
      shard_capacity_bytes_((capacity + kShardCount - 1) / kShardCount),
      total_used_bytes_(0),
      thread_pool_(
          std::make_unique<utils::TaskThreadPool<>>("disk_cache_manager")),
      layout_(std::move(layout)),
      queue_id_({0}),
      vars_(std::make_unique<DiskCacheManagerMetrics>(layout_->CacheIndex())) {
  for (auto& shard : shards_) {
    shard.cached_lru = std::make_unique<LRUCache>();
  }
  Init();
}

// for restart
void DiskCacheManager::Init() {
  total_used_bytes_.store(0, std::memory_order_relaxed);
  stage_full_ = false;
  cache_full_ = false;

  for (auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    shard.cached_lru->Clear();
    shard.entries.clear();
    shard.used_bytes = 0;

    CHECK_NOTNULL(shard.cached_lru);
    CHECK_EQ(shard.cached_lru->Size(), 0)
        << "Cached LRU size should be zero at startup.";
    CHECK_EQ(shard.entries.size(), 0)
        << "Cache entries should be empty at startup.";
  }

  CHECK_EQ(total_used_bytes_, 0) << "Used bytes should be zero at startup.";
  CHECK_EQ(stage_full_, 0) << "Stage full should be false at startup.";
  CHECK_EQ(cache_full_, 0) << "Cache full should be false at startup.";
}

void DiskCacheManager::Start() {
  if (running_) {
    return;
  }

  LOG(INFO) << "Disk cache manager is starting...";

  Init();

  CHECK_EQ(thread_pool_->Start(2), 0);

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &queue_options,
                                             HandleTask, this));

  running_ = true;

  thread_pool_->Enqueue(&DiskCacheManager::CheckFreeSpace, this);
  thread_pool_->Enqueue(&DiskCacheManager::CleanupExpire, this);

  LOG(INFO) << absl::StrFormat(
      "Disk cache manager is up: dir = %s, capacity = %.2lf MiB, "
      "free_space_ratio = %.2lf, cache_expire_s = %lu",
      GetRootDir(), capacity_bytes_ * 1.0 / kMiB, FLAGS_free_space_ratio,
      FLAGS_cache_expire_s);

  CHECK_RUNNING("Disk cache manager");
}

void DiskCacheManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Disk cache manager is shutting down...";

  thread_pool_->Stop();
  CHECK_EQ(bthread::execution_queue_stop(queue_id_), 0);
  CHECK_EQ(bthread::execution_queue_join(queue_id_), 0);

  LOG(INFO) << "Disk cache manager is down.";
}

void DiskCacheManager::AddStaging(const CacheKey& key,
                                  const CacheValue& value) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.entries.find(filename);
  if (iter == shard.entries.end()) {
    shard.entries.emplace(filename,
                          CacheEntry(key, value, CacheEntryState::kStaging));
    UpdateUsageLocked(shard, 1, static_cast<int64_t>(value.size));
    vars_->stage_blocks << 1;
  } else {
    auto& entry = iter->second;
    if (entry.state == CacheEntryState::kCached) {
      CacheValue deleted;
      CHECK(shard.cached_lru->Delete(entry.key, &deleted));
      vars_->stage_blocks << 1;
    }

    int64_t delta = static_cast<int64_t>(value.size) -
                    static_cast<int64_t>(entry.value.size);
    entry = CacheEntry(key, value, CacheEntryState::kStaging);
    UpdateUsageLocked(shard, 0, delta);
  }

  CleanupFullIfNeededLocked(shard);
}

void DiskCacheManager::PromoteStagingToCached(const CacheKey& key) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.entries.find(filename);
  CHECK(iter != shard.entries.end());
  auto& entry = iter->second;
  CHECK(entry.state == CacheEntryState::kStaging);

  entry.state = CacheEntryState::kCached;
  shard.cached_lru->Add(entry.key, entry.value);
  vars_->stage_blocks << -1;

  CleanupFullIfNeededLocked(shard);
}

void DiskCacheManager::AddCached(const CacheKey& key, const CacheValue& value) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.entries.find(filename);
  if (iter == shard.entries.end()) {
    shard.entries.emplace(filename,
                          CacheEntry(key, value, CacheEntryState::kCached));
    shard.cached_lru->Add(key, value);
    UpdateUsageLocked(shard, 1, static_cast<int64_t>(value.size));
  } else {
    auto& entry = iter->second;
    if (entry.state == CacheEntryState::kStaging) {
      // A not-yet-uploaded block is pinned. A read-cache insert for the same
      // key must not turn it into an evictable block.
      return;
    }

    CacheValue deleted;
    CHECK(shard.cached_lru->Delete(entry.key, &deleted));
    int64_t delta = static_cast<int64_t>(value.size) -
                    static_cast<int64_t>(entry.value.size);
    entry = CacheEntry(key, value, CacheEntryState::kCached);
    shard.cached_lru->Add(key, value);
    UpdateUsageLocked(shard, 0, delta);
  }

  CleanupFullIfNeededLocked(shard);
}

void DiskCacheManager::DeleteCached(const CacheKey& key) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.entries.find(filename);
  if (iter == shard.entries.end() ||
      iter->second.state == CacheEntryState::kStaging) {
    return;
  }

  CacheValue deleted;
  if (!shard.cached_lru->Delete(iter->second.key, &deleted)) {
    LOG(WARNING) << "Cached entry is missing from LRU, key=" << filename;
  }
  RemoveEntryLocked(shard, filename);
}

bool DiskCacheManager::Exist(const CacheKey& key) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.entries.find(filename);
  if (iter == shard.entries.end()) {
    return false;
  }

  auto& entry = iter->second;
  if (entry.state == CacheEntryState::kCached) {
    CacheValue value;
    if (shard.cached_lru->Get(entry.key, &value)) {
      entry.value = value;
    } else {
      LOG(WARNING) << "Cached entry is missing from LRU, key=" << filename;
    }
  } else {
    entry.value.atime = iutil::TimeNow();
  }
  return true;
}

bool DiskCacheManager::StageFull() const {
  return stage_full_.load(std::memory_order_relaxed);
}

bool DiskCacheManager::CacheFull() const {
  return cache_full_.load(std::memory_order_relaxed);
}

void DiskCacheManager::CheckFreeSpace() {
  CHECK_RUNNING("Disk cache manager");

  struct iutil::StatFS stat;
  uint64_t want_free_bytes, want_free_files;
  std::string root_dir = GetRootDir();

  while (running_.load(std::memory_order_relaxed)) {
    auto status = iutil::StatFS(root_dir, &stat);
    if (!status.ok()) {
      LOG(WARNING) << "Fail to check disk free space, dir=" << root_dir
                   << ", status=" << status.ToString()
                   << ", retry after 1 second...";
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    double br = stat.free_bytes_ratio;
    double fr = stat.free_files_ratio;
    double cfg = FLAGS_free_space_ratio;
    bool cache_full = br < cfg || fr < cfg;
    bool stage_full = (br < cfg / 2) || (fr < cfg / 2);
    cache_full_.store(cache_full, std::memory_order_release);
    stage_full_.store(stage_full, std::memory_order_release);
    vars_->cache_full.set_value(cache_full);
    vars_->stage_full.set_value(stage_full);

    if (cache_full) {
      LOG_EVERY_SECOND(WARNING) << absl::StrFormat(
          "Disk usage is so high: dir(%s), space%%(%.2lf/%.2lf), "
          "inode%%(%.2lf/%.2lf), used(%.2lf MiB vs %.2lf MiB), "
          "stop(cache=%s,stage=%s)",
          root_dir, (1.0 - br) * 100, (1.0 - cfg) * 100, (1.0 - fr) * 100,
          (1.0 - cfg) * 100,
          total_used_bytes_.load(std::memory_order_relaxed) * 1.0 / kMiB,
          stat.total_bytes * (1.0 - br) / kMiB, cache_full ? "Y" : "N",
          stage_full ? "Y" : "N");

      want_free_bytes = (br < cfg) ? stat.total_bytes * (cfg - br) : 0;
      want_free_files = (fr < cfg) ? stat.total_files * (cfg - fr) : 0;
      LOG(INFO) << absl::StrFormat(
          "Trigger delete block for disk full: "
          "used = %.2lf MiB, want free %.2lf MiB %llu files",
          total_used_bytes_.load(std::memory_order_relaxed) * 1.0 / kMiB,
          want_free_bytes * 1.0 / kMiB, want_free_files);
      CleanupAllShardsFull(want_free_bytes, want_free_files);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// assumes shard.mutex is held
void DiskCacheManager::CleanupFullIfNeededLocked(Shard& shard) {
  if (shard.used_bytes < shard_capacity_bytes_) {
    return;
  }

  uint64_t goal_bytes = shard_capacity_bytes_ * 95 / 100;
  uint64_t want_free_bytes =
      shard.used_bytes > goal_bytes ? shard.used_bytes - goal_bytes : 0;
  uint64_t want_free_files = shard.cached_lru->Size() * 5 / 100;
  LOG_EVERY_SECOND(INFO) << absl::StrFormat(
      "Trigger delete block for size reach capacity: "
      "shard used = %.2lf MiB, shard capacity = %.2lf MiB, "
      "want free %.2lf MiB %llu files",
      shard.used_bytes * 1.0 / kMiB, shard_capacity_bytes_ * 1.0 / kMiB,
      want_free_bytes * 1.0 / kMiB, want_free_files);
  CleanupFullLocked(shard, want_free_bytes, want_free_files);
}

// assumes shard.mutex is held
void DiskCacheManager::CleanupFullLocked(Shard& shard, uint64_t want_free_bytes,
                                         uint64_t want_free_files) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  auto to_del = shard.cached_lru->Evict([&](const CacheValue& value) {
    if (freed_bytes >= want_free_bytes && freed_files >= want_free_files) {
      return FilterStatus::kFinish;
    }

    freed_bytes += value.size;
    freed_files++;
    return FilterStatus::kEvictIt;
  });

  for (const auto& item : to_del) {
    RemoveEntryLocked(shard, item.key.Filename());
  }

  if (!to_del.empty()) {
    CHECK_EQ(0, bthread::execution_queue_execute(
                    queue_id_, ToDel{std::move(to_del), "cache full"}));
  }
}

// spread the whole-disk want_free across all shards, locking each in turn
void DiskCacheManager::CleanupAllShardsFull(uint64_t want_free_bytes,
                                            uint64_t want_free_files) {
  uint64_t per_shard_bytes = (want_free_bytes + kShardCount - 1) / kShardCount;
  uint64_t per_shard_files = (want_free_files + kShardCount - 1) / kShardCount;
  for (auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    CleanupFullLocked(shard, per_shard_bytes, per_shard_files);
  }
}

void DiskCacheManager::CleanupExpire() {
  CHECK_RUNNING("Disk cache manager");

  while (running_.load(std::memory_order_relaxed)) {
    auto cache_expire_s = FLAGS_cache_expire_s;
    if (cache_expire_s == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    auto now = iutil::TimeNow();
    for (auto& shard : shards_) {
      CacheItems to_del;
      uint64_t num_checks = 0;
      {
        BAIDU_SCOPED_LOCK(shard.mutex);
        to_del = shard.cached_lru->Evict([&](const CacheValue& value) {
          // per-shard check cap keeps the total scan budget on par with the
          // pre-shard single-LRU 1e3 limit
          if (++num_checks > (1e3 / kShardCount) + 1) {
            return FilterStatus::kFinish;
          } else if (value.atime + cache_expire_s > now) {
            return FilterStatus::kSkip;
          }
          return FilterStatus::kEvictIt;
        });

        for (const auto& item : to_del) {
          RemoveEntryLocked(shard, item.key.Filename());
        }
      }

      if (!to_del.empty()) {
        CHECK_EQ(0, bthread::execution_queue_execute(
                        queue_id_, ToDel{std::move(to_del), "cache expired"}));
      }
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_cache_cleanup_expire_interval_ms));
  }
}

int DiskCacheManager::HandleTask(void* meta,
                                 bthread::TaskIterator<ToDel>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<DiskCacheManager*>(meta);
  for (; iter; iter++) {
    auto& to_del = *iter;
    self->DeleteBlocks(to_del);
  }
  return 0;
}

void DiskCacheManager::DeleteBlocks(const ToDel& to_del) {
  butil::Timer timer;
  uint64_t num_deleted = 0, bytes_freed = 0;

  timer.start();
  for (const auto& item : to_del.items) {
    CacheKey key = item.key;
    CacheValue value = item.value;
    std::string cache_path = GetCachePath(key);
    auto status = iutil::Unlink(cache_path);
    if (status.IsNotFound()) {
      LOG(WARNING) << "Block file already deleted, path=" << cache_path;
      continue;
    } else if (!status.ok()) {
      LOG(ERROR) << "Delete block file failed for " << to_del.reason
                 << ": path = " << cache_path
                 << ", status = " << status.ToString();
      continue;
    }

    VLOG(3) << "Delete block file success for " << to_del.reason
            << ": path = " << cache_path << ", free " << value.size
            << " bytes.";

    num_deleted++;
    bytes_freed += value.size;
  }
  timer.stop();

  LOG(INFO) << absl::StrFormat(
      "%d cache blocks deleted for %s, free %.2lf MiB, costs %.6lf seconds.",
      num_deleted, to_del.reason, static_cast<double>(bytes_freed) / kMiB,
      timer.u_elapsed() / 1e6);
}

// assumes shard.mutex is held
void DiskCacheManager::RemoveEntryLocked(Shard& shard,
                                         const std::string& filename) {
  auto iter = shard.entries.find(filename);
  if (iter == shard.entries.end()) {
    return;
  }

  const auto state = iter->second.state;
  const auto size = iter->second.value.size;
  shard.entries.erase(iter);
  if (state == CacheEntryState::kStaging) {
    vars_->stage_blocks << -1;
  }
  UpdateUsageLocked(shard, -1, -static_cast<int64_t>(size));
}

// assumes shard.mutex is held
void DiskCacheManager::UpdateUsageLocked(Shard& shard, int64_t n,
                                         int64_t used_bytes) {
  if (used_bytes < 0) {
    auto delta = static_cast<uint64_t>(-used_bytes);
    CHECK_GE(shard.used_bytes, delta);
    shard.used_bytes -= delta;
  } else {
    shard.used_bytes += static_cast<uint64_t>(used_bytes);
  }

  int64_t total =
      total_used_bytes_.fetch_add(used_bytes, std::memory_order_relaxed) +
      used_bytes;
  vars_->used_bytes.set_value(total);
  vars_->cache_blocks << n;
  vars_->cache_bytes << used_bytes;
}

std::string DiskCacheManager::GetRootDir() const {
  return layout_->GetRootDir();
}

std::string DiskCacheManager::GetCachePath(const CacheKey& key) const {
  return layout_->GetCachePath(key);
}

}  // namespace cache
}  // namespace dingofs
