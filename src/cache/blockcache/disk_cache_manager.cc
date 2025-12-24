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

#include "cache/blockcache/disk_cache_manager.h"

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

DEFINE_double(free_space_ratio, 0.1, "ratio of free space of total disk space");

DEFINE_uint32(cache_expire_s, 259200,
              "expiration time for cache blocks in seconds");
DEFINE_validator(cache_expire_s, brpc::PassValidate);

DEFINE_uint32(cache_cleanup_expire_interval_ms, 1000,
              "interval for cleaning up expired cache blocks in milliseconds");
DEFINE_validator(cache_cleanup_expire_interval_ms, brpc::PassValidate);

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   DiskCacheLayoutSPtr layout)
    : running_(false),
      capacity_bytes_(capacity),
      cached_blocks_(std::make_unique<LRUCache>()),
      thread_pool_(
          std::make_unique<utils::TaskThreadPool<>>("disk_cache_manager")),
      layout_(layout),
      queue_id_({0}),
      vars_(std::make_unique<DiskCacheManagerVarsCollector>(
          layout_->CacheIndex())) {
  Init();
}

// for restart
void DiskCacheManager::Init() {
  used_bytes_ = 0;
  stage_full_ = false;
  cache_full_ = false;
  cached_blocks_->Clear();
  staging_blocks_.clear();

  CHECK_EQ(used_bytes_, 0) << "Used bytes should be zero at startup.";
  CHECK_EQ(stage_full_, 0) << "Stage full should be false at startup.";
  CHECK_EQ(cache_full_, 0) << "Cache full should be false at startup.";
  CHECK_NOTNULL(cached_blocks_);
  CHECK_EQ(cached_blocks_->Size(), 0)
      << "Cached blocks size should be zero at startup.";
  CHECK_EQ(staging_blocks_.size(), 0)
      << "Staging blocks should be empty at startup.";
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

void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value,
                           BlockPhase phase) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  if (phase == BlockPhase::kStaging) {
    staging_blocks_.emplace(key.Filename(), value);
    UpdateUsage(1, value.size);
    vars_->stage_blocks << 1;
  } else if (phase == BlockPhase::kUploaded) {
    auto iter = staging_blocks_.find(key.Filename());
    CHECK(iter != staging_blocks_.end());
    cached_blocks_->Add(key, iter->second);
    staging_blocks_.erase(iter);
    vars_->stage_blocks << -1;
  } else {  // cached
    cached_blocks_->Add(key, value);
    UpdateUsage(1, value.size);
  }

  if (used_bytes_ >= capacity_bytes_) {
    uint64_t want_free_bytes = used_bytes_ - (capacity_bytes_ * 0.95);
    uint64_t want_free_files = cached_blocks_->Size() * 0.05;
    LOG(INFO) << absl::StrFormat(
        "Trigger delete block for size reach capacity: "
        "used = %.2lf MiB, capacity = %.2lf MiB, "
        "want free %.2lf MiB %llu files",
        used_bytes_ * 1.0 / kMiB, capacity_bytes_ * 1.0 / kMiB,
        want_free_bytes * 1.0 / kMiB, want_free_files);
    CleanupFull(want_free_bytes, want_free_files);
  }
}

void DiskCacheManager::Delete(const CacheKey& key) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  CacheValue value;
  if (cached_blocks_->Delete(key, &value)) {  // exist
    UpdateUsage(-1, -value.size);
  }
}

// FIXME: lock contention
bool DiskCacheManager::Exist(const CacheKey& key) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  CacheValue value;
  return cached_blocks_->Get(key, &value) ||
         staging_blocks_.find(key.Filename()) != staging_blocks_.end();
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
  uint64_t goal_bytes, goal_files;
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
          (1.0 - cfg) * 100, used_bytes_ * 1.0 / kMiB,
          stat.total_bytes * (1.0 - br) / kMiB, cache_full ? "Y" : "N",
          stage_full ? "Y" : "N");

      std::lock_guard<bthread::Mutex> lk(mutex_);
      want_free_bytes = (br < cfg) ? stat.total_bytes * (cfg - br) : 0;
      want_free_files = (fr < cfg) ? stat.total_files * (cfg - fr) : 0;
      LOG(INFO) << absl::StrFormat(
          "Trigger delete block for disk full: "
          "used = %.2lf MiB, want free %.2lf MiB %llu files",
          used_bytes_ * 1.0 / kMiB, want_free_bytes * 1.0 / kMiB,
          want_free_files);
      CleanupFull(want_free_bytes, want_free_files);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// protected by mutex
void DiskCacheManager::CleanupFull(uint64_t want_free_bytes,
                                   uint64_t want_free_files) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  auto to_del = cached_blocks_->Evict([&](const CacheValue& value) {
    if (freed_bytes >= want_free_bytes && freed_files >= want_free_files) {
      return FilterStatus::kFinish;
    }

    freed_bytes += value.size;
    freed_files++;
    UpdateUsage(-1, -value.size);
    return FilterStatus::kEvictIt;
  });

  if (!to_del.empty()) {
    CHECK_EQ(0, bthread::execution_queue_execute(queue_id_,
                                                 ToDel{to_del, "cache full"}));
  }
}

void DiskCacheManager::CleanupExpire() {
  CHECK_RUNNING("Disk cache manager");

  CacheItems to_del;
  while (running_.load(std::memory_order_relaxed)) {
    uint64_t num_checks = 0;
    auto now = iutil::TimeNow();
    auto cache_expire_s = FLAGS_cache_expire_s;
    if (cache_expire_s == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    {
      std::lock_guard<bthread::Mutex> lk(mutex_);
      to_del = cached_blocks_->Evict([&](const CacheValue& value) {
        if (++num_checks > 1e3) {
          return FilterStatus::kFinish;
        } else if (value.atime + cache_expire_s > now) {
          return FilterStatus::kSkip;
        }
        UpdateUsage(-1, -value.size);
        return FilterStatus::kEvictIt;
      });
    }

    if (!to_del.empty()) {
      CHECK_EQ(0, bthread::execution_queue_execute(
                      queue_id_, ToDel{to_del, "cache expired"}));
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
      LOG(WARNING) << "Block file already deleted: path = " << cache_path;
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

void DiskCacheManager::UpdateUsage(int64_t n, int64_t used_bytes) {
  used_bytes_ += used_bytes;
  vars_->used_bytes.set_value(used_bytes_);
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
