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

#include "client/blockcache/disk_cache_manager.h"

#include <butil/time.h>

#include <chrono>
#include <memory>

#include "base/math/math.h"
#include "base/time/time.h"
#include "client/blockcache/disk_cache_metric.h"
#include "client/blockcache/lru_cache.h"
#include "client/blockcache/lru_common.h"
#include "client/common/dynamic_config.h"

namespace dingofs {
namespace client {
namespace blockcache {

USING_FLAG(disk_cache_expire_second);
USING_FLAG(disk_cache_cleanup_expire_interval_millsecond);
USING_FLAG(disk_cache_free_space_ratio);

using base::math::kMiB;
using base::string::StrFormat;
using base::time::TimeNow;
using butil::Timer;
using utils::LockGuard;

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   std::shared_ptr<DiskCacheLayout> layout,
                                   std::shared_ptr<LocalFileSystem> fs,
                                   std::shared_ptr<DiskCacheMetric> metric)
    : used_bytes_(0),
      capacity_(capacity),
      stage_full_(false),
      cache_full_(false),
      running_(false),
      layout_(layout),
      fs_(fs),
      lru_(std::make_unique<LRUCache>()),
      metric_(metric),
      task_pool_(std::make_unique<TaskThreadPool<>>("disk_cache_manager")) {
  mq_ = std::make_unique<MessageQueueType>("delete_block_queue", 10);
  mq_->Subscribe([&](MessageType message) {
    DeleteBlocks(message.first, message.second);
  });
}

void DiskCacheManager::Start() {
  if (running_.exchange(true)) {
    return;  // already running
  }

  used_bytes_ = 0;  // For restart
  mq_->Start();
  task_pool_->Start(2);
  task_pool_->Enqueue(&DiskCacheManager::CheckFreeSpace, this);
  task_pool_->Enqueue(&DiskCacheManager::CleanupExpire, this);
  LOG(INFO) << "Disk cache manager start, capacity=" << capacity_
            << ", free_space_ratio=" << FLAGS_disk_cache_free_space_ratio
            << ", cache_expire_second=" << FLAGS_disk_cache_expire_second;
}

void DiskCacheManager::Stop() {
  if (!running_.exchange(false)) {
    return;  // already stopped
  }

  LOG(INFO) << "Stop disk cache manager thread...";
  task_pool_->Stop();
  mq_->Stop();
  lru_->Clear();
  LOG(INFO) << "Disk cache manager thread stopped.";
}

void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value) {
  LockGuard lk(mutex_);
  lru_->Add(key, value);
  UpdateUsage(1, value.size);
  if (used_bytes_ >= capacity_) {
    uint64_t goal_bytes = capacity_ * 0.95;
    uint64_t goal_files = lru_->Size() * 0.95;
    CleanupFull(goal_bytes, goal_files);
  }
}

Status DiskCacheManager::Get(const CacheKey& key, CacheValue* value) {
  LockGuard lk(mutex_);
  if (lru_->Get(key, value)) {
    return Status::OK();
  }
  return Status::NotFound("cache not found");
}

void DiskCacheManager::Delete(const CacheKey& key) {
  LockGuard lk(mutex_);
  CacheValue value;
  if (lru_->Delete(key, &value)) {  // exist
    UpdateUsage(-1, -value.size);
  }
}

bool DiskCacheManager::StageFull() const {
  return stage_full_.load(std::memory_order_acquire);
}

bool DiskCacheManager::CacheFull() const {
  return cache_full_.load(std::memory_order_acquire);
}

void DiskCacheManager::CheckFreeSpace() {
  uint64_t goal_bytes, goal_files;
  struct LocalFileSystem::StatDisk stat;
  std::string root_dir = layout_->GetRootDir();

  while (running_.load(std::memory_order_relaxed)) {
    auto status = fs_->GetDiskUsage(root_dir, &stat);
    if (!status.ok()) {
      LOG(ERROR) << "Check free space failed: " << status.ToString();
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    double br = stat.free_bytes_ratio;
    double fr = stat.free_files_ratio;
    double cfg = FLAGS_disk_cache_free_space_ratio;
    bool cache_full = br < cfg || fr < cfg;
    bool stage_full = (br < cfg / 2) || (fr < cfg / 2);
    cache_full_.store(cache_full, std::memory_order_release);
    stage_full_.store(stage_full, std::memory_order_release);
    metric_->SetCacheFull(cache_full_);
    metric_->SetStageFull(stage_full_);

    if (cache_full) {
      double watermark = 1.0 - cfg;

      LOG_EVERY_SECOND(WARNING) << StrFormat(
          "Disk usage is so high: dir=%s, watermark=%.2f%%, "
          "bytes_usage=%.2f%%, files_usage=%.2f%%, stop_cache=%c, "
          "stop_stage=%c.",
          root_dir, watermark * 100, (1.0 - br) * 100, (1.0 - fr) * 100,
          cache_full ? 'Y' : 'N', stage_full ? 'Y' : 'N');

      LockGuard lk(mutex_);
      goal_bytes = stat.total_bytes * watermark;
      goal_files = stat.total_files * watermark;
      CleanupFull(goal_bytes, goal_files);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// protect by mutex
void DiskCacheManager::CleanupFull(uint64_t goal_bytes, uint64_t goal_files) {
  auto to_del = lru_->Evict([&](const CacheValue& value) {
    if (used_bytes_ <= goal_bytes && lru_->Size() <= goal_files) {
      return FilterStatus::kFinish;
    }
    UpdateUsage(-1, -value.size);
    return FilterStatus::kEvictIt;
  });

  if (to_del.size() > 0) {
    mq_->Publish({to_del, DeleteFrom::kCacheFull});
  }
}

void DiskCacheManager::CleanupExpire() {
  CacheItems to_del;
  while (running_.load(std::memory_order_relaxed)) {
    uint64_t num_checks = 0;
    auto now = TimeNow();
    if (FLAGS_disk_cache_expire_second == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    {
      LockGuard lk(mutex_);
      to_del = lru_->Evict([&](const CacheValue& value) {
        if (++num_checks > 1e3) {
          return FilterStatus::kFinish;
        } else if (value.atime + FLAGS_disk_cache_expire_second > now) {
          return FilterStatus::kSkip;
        }
        UpdateUsage(-1, -value.size);
        return FilterStatus::kEvictIt;
      });
    }

    if (to_del.size() > 0) {
      mq_->Publish({to_del, DeleteFrom::kCacheExpired});
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(
        FLAGS_disk_cache_cleanup_expire_interval_millsecond));
  }
}

void DiskCacheManager::DeleteBlocks(const CacheItems& to_del, DeleteFrom from) {
  Timer timer;
  uint64_t num_deleted = 0, bytes_freed = 0;

  timer.start();
  for (const auto& item : to_del) {
    CacheKey key = item.key;
    CacheValue value = item.value;
    std::string cache_path = GetCachePath(key);
    auto status = fs_->RemoveFile(cache_path);
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
            << StrFrom(from) << ", free " << value.size << " bytes.";

    num_deleted++;
    bytes_freed += value.size;
  }
  timer.stop();

  LOG(INFO) << StrFormat(
      "%d cache blocks deleted for %s, free %.2f MiB, costs %.6f seconds.",
      num_deleted, StrFrom(from), static_cast<double>(bytes_freed) / kMiB,
      timer.u_elapsed() / 1e6);
}

void DiskCacheManager::UpdateUsage(int64_t n, int64_t bytes) {
  used_bytes_ += bytes;
  metric_->AddCacheBlock(n, bytes);
  metric_->SetUsedBytes(used_bytes_);
}

std::string DiskCacheManager::GetCachePath(const CacheKey& key) {
  return layout_->GetCachePath(key);
}

std::string DiskCacheManager::StrFrom(DeleteFrom from) {
  if (from == DeleteFrom::kCacheFull) {
    return "cache full";
  } else {
    return "cache expired";
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
