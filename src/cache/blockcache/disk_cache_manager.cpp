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
#include <glog/logging.h>

#include <atomic>

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/common/type.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

DEFINE_double(free_space_ratio, 0.1, "Ratio of free space of total disk space");

DEFINE_uint32(cache_expire_s, 259200,
              "Expiration time for cache blocks in seconds");
DEFINE_validator(cache_expire_s, brpc::PassValidate);

DEFINE_uint32(cleanup_expire_interval_ms, 1000,
              "Interval for cleaning up expired cache blocks in milliseconds");
DEFINE_validator(cleanup_expire_interval_ms, brpc::PassValidate);

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   DiskCacheLayoutSPtr layout,
                                   DiskCacheMetricSPtr metric)
    : running_(false),
      used_bytes_(0),
      capacity_bytes_(capacity),
      stage_full_(false),
      cache_full_(false),
      layout_(layout),
      cached_blocks_(std::make_unique<LRUCache>()),
      mq_(std::make_unique<MessageQueueType>("delete_block_queue", 10)),
      thread_pool_(std::make_unique<TaskThreadPool>("disk_cache_manager")),
      metric_(metric) {}

void DiskCacheManager::Start() {
  CHECK_NOTNULL(layout_);
  CHECK_NOTNULL(cached_blocks_);
  CHECK(staging_blocks_.empty());
  CHECK_NOTNULL(mq_);
  CHECK_NOTNULL(thread_pool_);
  CHECK_NOTNULL(metric_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Disk cache manager is starting...";

  mq_->Subscribe([&](MessageType message) {
    DeleteBlocks(message.first, message.second);
  });
  mq_->Start();

  running_ = true;

  CHECK_EQ(thread_pool_->Start(2), 0);
  thread_pool_->Enqueue(&DiskCacheManager::CheckFreeSpace, this);
  thread_pool_->Enqueue(&DiskCacheManager::CleanupExpire, this);

  LOG(INFO) << absl::StrFormat(
      "Disk cache manager is up: capacity = %.2lf MiB, "
      "free_space_ratio = %.2lf, cache_expire_s = %d",
      capacity_bytes_ * 1.0 / kMiB, FLAGS_free_space_ratio,
      FLAGS_cache_expire_s);

  CHECK_RUNNING("Disk cache manager");
  CHECK_EQ(used_bytes_, 0) << "Used bytes should be zero at startup.";
  CHECK_EQ(stage_full_, 0) << "Stage full should be false at startup.";
  CHECK_EQ(cache_full_, 0) << "Cache full should be false at startup.";
  CHECK_EQ(cached_blocks_->Size(), 0)
      << "Cached blocks size should be zero at startup.";
  CHECK(staging_blocks_.empty())
      << "Staging blocks should be empty at startup.";
}

void DiskCacheManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Disk cache manager is shutting down...";

  thread_pool_->Stop();
  mq_->Stop();
  cached_blocks_->Clear();
  staging_blocks_.clear();

  // Reset for disk cache restart
  used_bytes_ = 0;
  stage_full_ = false;
  cache_full_ = false;

  LOG(INFO) << "Disk cache manager is down.";

  CHECK_DOWN("Disk cache manager");
}

void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value,
                           BlockPhase phase) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  if (phase == BlockPhase::kStaging) {
    staging_blocks_.emplace(key.Filename(), value);
    UpdateUsage(1, value.size);
    metric_->stage_blocks << 1;
  } else if (phase == BlockPhase::kUploaded) {
    auto iter = staging_blocks_.find(key.Filename());
    CHECK(iter != staging_blocks_.end());
    cached_blocks_->Add(key, iter->second);
    staging_blocks_.erase(iter);
    metric_->stage_blocks << -1;
  } else {  // cached
    cached_blocks_->Add(key, value);
    UpdateUsage(1, value.size);
  }

  if (used_bytes_ >= capacity_bytes_) {
    uint64_t goal_bytes = capacity_bytes_ * 0.95;
    uint64_t goal_files = cached_blocks_->Size() * 0.95;
    CleanupFull(goal_bytes, goal_files);
  }
}

void DiskCacheManager::Delete(const CacheKey& key) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  CacheValue value;
  if (cached_blocks_->Delete(key, &value)) {  // exist
    UpdateUsage(-1, -value.size);
  }
}

bool DiskCacheManager::Exist(const CacheKey& key) {
  std::lock_guard<BthreadMutex> lk(mutex_);
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

  struct FSStat stat;
  uint64_t goal_bytes, goal_files;
  std::string root_dir = GetRootDir();

  while (running_.load(std::memory_order_relaxed)) {
    auto status = Helper::StatFS(root_dir, &stat);
    if (!status.ok()) {
      LOG(ERROR) << "Check disk free space failed: " << status.ToString();
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
    metric_->cache_full.set_value(cache_full);
    metric_->stage_full.set_value(stage_full);

    if (cache_full) {
      double watermark = 1.0 - cfg;

      LOG_EVERY_SECOND(WARNING) << absl::StrFormat(
          "Disk usage is so high: dir = %s, watermark = %.2f%%, "
          "bytes_usage = %.2f%%, files_usage = %.2f%%, stop_cache = %s, "
          "stop_stage = %s.",
          root_dir, watermark * 100, (1.0 - br) * 100, (1.0 - fr) * 100,
          cache_full ? "YES" : "NO", stage_full ? "YES" : "NO");

      std::lock_guard<BthreadMutex> lk(mutex_);
      goal_bytes = stat.total_bytes * watermark;
      goal_files = stat.total_files * watermark;
      CleanupFull(goal_bytes, goal_files);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// Protected by mutex
void DiskCacheManager::CleanupFull(uint64_t goal_bytes, uint64_t goal_files) {
  auto to_del = cached_blocks_->Evict([&](const CacheValue& value) {
    if (used_bytes_ <= goal_bytes && cached_blocks_->Size() <= goal_files) {
      return FilterStatus::kFinish;
    }
    UpdateUsage(-1, -value.size);
    return FilterStatus::kEvictIt;
  });

  if (!to_del.empty()) {
    mq_->Publish({to_del, DeletionReason::kCacheFull});
  }
}

void DiskCacheManager::CleanupExpire() {
  CHECK_RUNNING("Disk cache manager");

  CacheItems to_del;
  while (running_.load(std::memory_order_relaxed)) {
    uint64_t num_checks = 0;
    auto now = utils::TimeNow();
    auto cache_expire_s = FLAGS_cache_expire_s;
    if (cache_expire_s == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    {
      std::lock_guard<BthreadMutex> lk(mutex_);
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
      mq_->Publish({to_del, DeletionReason::kCacheExpired});
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_cleanup_expire_interval_ms));
  }
}

void DiskCacheManager::DeleteBlocks(const CacheItems& to_del,
                                    DeletionReason reason) {
  butil::Timer timer;
  uint64_t num_deleted = 0, bytes_freed = 0;

  timer.start();
  for (const auto& item : to_del) {
    CacheKey key = item.key;
    CacheValue value = item.value;
    std::string cache_path = GetCachePath(key);
    auto status = Helper::RemoveFile(cache_path);
    if (status.IsNotFound()) {
      LOG(WARNING) << "Cache block (path=" << cache_path
                   << ") already deleted.";
      continue;
    } else if (!status.ok()) {
      LOG(ERROR) << "Delete cache block (path=" << cache_path
                 << ") failed: " << status.ToString();
      continue;
    }

    VLOG(3) << "Cache block (path=" << cache_path << ") deleted for "
            << ToString(reason) << ", free " << value.size << " bytes.";

    num_deleted++;
    bytes_freed += value.size;
  }
  timer.stop();

  LOG(INFO) << absl::StrFormat(
      "%d cache blocks deleted for %s, free %.2f MiB, costs %.6f seconds.",
      num_deleted, ToString(reason), static_cast<double>(bytes_freed) / kMiB,
      timer.u_elapsed() / 1e6);
}

void DiskCacheManager::UpdateUsage(int64_t n, int64_t used_bytes) {
  used_bytes_ += used_bytes;
  metric_->used_bytes.set_value(used_bytes_);
  metric_->cache_blocks << n;
  metric_->cache_bytes << used_bytes;
}

std::string DiskCacheManager::GetRootDir() const {
  return layout_->GetRootDir();
}

std::string DiskCacheManager::GetCachePath(const CacheKey& key) const {
  return layout_->GetCachePath(key);
}

std::string DiskCacheManager::ToString(DeletionReason reason) const {
  switch (reason) {
    case DeletionReason::kCacheFull:
      return "cache full";
    case DeletionReason::kCacheExpired:
      return "cache expired";
    default:
      CHECK(false) << "Unknown deletion reason: " << static_cast<int>(reason);
  }
}

}  // namespace cache
}  // namespace dingofs
