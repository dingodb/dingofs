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
#include <utility>

#include "cache/common/macro.h"
#include "cache/iutil/file_util.h"
#include "cache/iutil/time_util.h"
#include "common/const.h"
#include "common/options/cache.h"
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

DiskCacheManager::DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout,
                                   std::string eviction_policy)
    : running_(false),
      capacity_bytes_(capacity),
      shard_capacity_bytes_((capacity + kShardCount - 1) / kShardCount),
      eviction_policy_(eviction_policy.empty() ? FLAGS_cache_eviction_policy
                                               : eviction_policy),
      now_sec_(0),
      total_used_bytes_(0),
      thread_pool_(
          std::make_unique<utils::TaskThreadPool<>>("disk_cache_manager")),
      layout_(std::move(layout)),
      queue_id_({0}),
      vars_(std::make_unique<DiskCacheManagerMetrics>(layout_->CacheIndex())) {
  vars_->eviction_policy.set_value(eviction_policy_);
  Init();
}

// for restart
void DiskCacheManager::Init() {
  total_used_bytes_.store(0, std::memory_order_relaxed);
  now_sec_.store(static_cast<uint32_t>(iutil::TimeNow().sec),
                 std::memory_order_relaxed);
  stage_full_ = false;
  cache_full_ = false;

  for (auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    shard.index.clear();
    shard.used_bytes = 0;
    shard.policy = NewEvictionPolicy(eviction_policy_);
    CHECK_NOTNULL(shard.policy);
    CHECK_EQ(shard.index.size(), 0)
        << "Cache index should be empty at startup.";
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
      "free_space_ratio = %.2lf, cache_expire_s = %lu, eviction_policy = %s",
      GetRootDir(), capacity_bytes_ * 1.0 / kMiB, FLAGS_free_space_ratio,
      FLAGS_cache_expire_s, eviction_policy_);

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

void DiskCacheManager::AddStaging(const BlockHandle& handle, uint32_t size,
                                  uint32_t atime_sec) {
  auto& shard = GetShard(handle);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto [iter, inserted] = shard.index.try_emplace(handle);
  CacheEntry& entry = iter->second;
  if (inserted) {
    entry.key = &iter->first;
    entry.size = size;
    entry.atime.store(atime_sec, std::memory_order_relaxed);
    entry.staged = true;
    UpdateUsageLocked(shard, 1, static_cast<int64_t>(size));
    vars_->stage_blocks << 1;
  } else {
    if (!entry.staged) {
      // A cached block is being overwritten by a new writeback: it must become
      // pinned again, so unlink it from the eviction policy.
      shard.policy->OnErase(&entry);
      entry.staged = true;
      vars_->stage_blocks << 1;
    }
    int64_t delta =
        static_cast<int64_t>(size) - static_cast<int64_t>(entry.size);
    entry.size = size;
    entry.atime.store(atime_sec, std::memory_order_relaxed);
    UpdateUsageLocked(shard, 0, delta);
  }

  CleanupFullIfNeededLocked(shard);
}

void DiskCacheManager::PromoteStagingToCached(const BlockHandle& handle) {
  auto& shard = GetShard(handle);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.index.find(handle);
  CHECK(iter != shard.index.end());
  CacheEntry& entry = iter->second;
  CHECK(entry.staged);

  entry.staged = false;
  shard.policy->OnInsert(&entry);
  vars_->stage_blocks << -1;

  CleanupFullIfNeededLocked(shard);
}

void DiskCacheManager::AddCached(const BlockHandle& handle, uint32_t size,
                                 uint32_t atime_sec) {
  auto& shard = GetShard(handle);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto [iter, inserted] = shard.index.try_emplace(handle);
  CacheEntry& entry = iter->second;
  if (inserted) {
    entry.key = &iter->first;
    entry.size = size;
    entry.atime.store(atime_sec, std::memory_order_relaxed);
    entry.staged = false;
    shard.policy->OnInsert(&entry);
    UpdateUsageLocked(shard, 1, static_cast<int64_t>(size));
  } else if (entry.staged) {
    // A not-yet-uploaded block is pinned. A read-cache insert for the same key
    // must not turn it into an evictable block.
    return;
  } else {
    int64_t delta =
        static_cast<int64_t>(size) - static_cast<int64_t>(entry.size);
    entry.size = size;
    entry.atime.store(atime_sec, std::memory_order_relaxed);
    shard.policy->OnAccess(&entry);
    UpdateUsageLocked(shard, 0, delta);
  }

  CleanupFullIfNeededLocked(shard);
}

void DiskCacheManager::DeleteCached(const BlockHandle& handle) {
  auto& shard = GetShard(handle);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.index.find(handle);
  if (iter == shard.index.end() || iter->second.staged) {
    return;
  }

  CacheEntry& entry = iter->second;
  shard.policy->OnErase(&entry);
  UpdateUsageLocked(shard, -1, -static_cast<int64_t>(entry.size));
  shard.index.erase(iter);
}

bool DiskCacheManager::Exist(const BlockHandle& handle) {
  auto& shard = GetShard(handle);
  BAIDU_SCOPED_LOCK(shard.mutex);

  auto iter = shard.index.find(handle);
  if (iter == shard.index.end()) {
    return false;
  }

  CacheEntry& entry = iter->second;
  entry.atime.store(now_sec_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
  if (!entry.staged) {
    shard.policy->OnAccess(&entry);
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
    now_sec_.store(static_cast<uint32_t>(iutil::TimeNow().sec),
                   std::memory_order_relaxed);

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
  uint64_t want_free_files = shard.index.size() * 5 / 100;
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
  CacheVictims victims;
  shard.policy->Evict(want_free_bytes, want_free_files, &victims);
  if (victims.empty()) {
    return;
  }

  uint64_t freed_bytes = FlushVictimsLocked(shard, victims, "cache full");
  vars_->evict_blocks << static_cast<int64_t>(victims.size());
  vars_->evict_bytes << static_cast<int64_t>(freed_bytes);
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

  // per-shard scan cap keeps the total budget on par with the pre-shard 1e3
  const uint64_t budget = static_cast<uint64_t>(1e3 / kShardCount) + 1;

  while (running_.load(std::memory_order_relaxed)) {
    auto cache_expire_s = FLAGS_cache_expire_s;
    if (cache_expire_s == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    auto now_sec = now_sec_.load(std::memory_order_relaxed);
    for (auto& shard : shards_) {
      CacheVictims victims;
      {
        BAIDU_SCOPED_LOCK(shard.mutex);
        shard.policy->EvictExpired(now_sec, cache_expire_s, budget, &victims);
        if (!victims.empty()) {
          FlushVictimsLocked(shard, victims, "cache expired");
          vars_->expire_blocks << static_cast<int64_t>(victims.size());
        }
      }
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_cache_cleanup_expire_interval_ms));
  }
}

// assumes shard.mutex is held
uint64_t DiskCacheManager::FlushVictimsLocked(Shard& shard,
                                              const CacheVictims& victims,
                                              const std::string& reason) {
  ToDel to_del;
  to_del.reason = reason;
  to_del.items.reserve(victims.size());

  uint64_t freed_bytes = 0;
  for (CacheEntry* victim : victims) {
    BlockHandle handle = *victim->key;  // copy before erase destroys the entry
    uint32_t size = victim->size;
    freed_bytes += size;
    UpdateUsageLocked(shard, -1, -static_cast<int64_t>(size));
    shard.index.erase(handle);
    to_del.items.push_back(DelItem{std::move(handle), size});
  }

  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, std::move(to_del)));
  return freed_bytes;
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
    std::string cache_path = GetCachePath(item.handle);
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
            << ": path = " << cache_path << ", free " << item.size << " bytes.";

    num_deleted++;
    bytes_freed += item.size;
  }
  timer.stop();

  LOG(INFO) << absl::StrFormat(
      "%d cache blocks deleted for %s, free %.2lf MiB, costs %.6lf seconds.",
      num_deleted, to_del.reason, static_cast<double>(bytes_freed) / kMiB,
      timer.u_elapsed() / 1e6);
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

std::string DiskCacheManager::GetCachePath(const BlockHandle& handle) const {
  return layout_->GetCachePath(handle);
}

}  // namespace cache
}  // namespace dingofs
