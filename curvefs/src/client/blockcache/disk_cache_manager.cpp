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

#include "curvefs/src/client/blockcache/disk_cache_manager.h"

#include <butil/time.h>

#include <memory>

#include "curvefs/src/base/math/math.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/blockcache/disk_cache_metric.h"
#include "curvefs/src/client/blockcache/lru_cache.h"
#include "curvefs/src/client/blockcache/lru_common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/common/dynamic_config.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(disk_cache_expire_second);
USING_FLAG(disk_cache_free_space_ratio);

using ::butil::Timer;
using ::curve::common::LockGuard;
using ::curvefs::base::math::kMiB;
using ::curvefs::base::string::StrFormat;
using ::curvefs::base::time::TimeNow;

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

BCACHE_ERROR DiskCacheManager::Get(const CacheKey& key, CacheValue* value) {
  LockGuard lk(mutex_);
  if (lru_->Get(key, value)) {
    return BCACHE_ERROR::OK;
  }
  return BCACHE_ERROR::NOT_FOUND;
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
    auto rc = fs_->GetDiskUsage(root_dir, &stat);
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Check free space failed: " << StrErr(rc);
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
      return FilterStatus::FINISH;
    }
    UpdateUsage(-1, -value.size);
    return FilterStatus::EVICT_IT;
  });

  if (to_del.size() > 0) {
    mq_->Publish({to_del, DeleteFrom::CACHE_FULL});
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
          return FilterStatus::FINISH;
        } else if (value.atime + FLAGS_disk_cache_expire_second > now) {
          return FilterStatus::SKIP;
        }
        UpdateUsage(-1, -value.size);
        return FilterStatus::EVICT_IT;
      });
    }

    if (to_del.size() > 0) {
      mq_->Publish({to_del, DeleteFrom::CACHE_EXPIRED});
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

void DiskCacheManager::DeleteBlocks(const CacheItems& to_del, DeleteFrom from) {
  Timer timer;
  uint64_t num_deleted = 0, bytes_freed = 0;

  timer.start();
  for (const auto& item : to_del) {
    CacheKey key = item.key;
    CacheValue value = item.value;
    auto rc = fs_->RemoveFile(GetCachePath(key));
    if (rc == BCACHE_ERROR::NOT_FOUND) {
      LOG(WARNING) << "Cache block " << key.Filename() << " already deleted.";
      continue;
    } else if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Delete cache block " << key.Filename()
                 << " failed: " << StrErr(rc);
      continue;
    }

    num_deleted++;
    bytes_freed += value.size;
  }
  timer.stop();

  LOG(INFO) << StrFormat(
      "%d cache blocks deleted for %s, free %.2f MiB, costs %.6f seconds.",
      num_deleted,
      from == DeleteFrom::CACHE_FULL ? "cache full" : "cache expired",
      static_cast<double>(bytes_freed) / kMiB, timer.u_elapsed() / 1e6);
}

void DiskCacheManager::UpdateUsage(int64_t n, int64_t bytes) {
  used_bytes_ += bytes;
  metric_->AddCacheBlock(n, bytes);
  metric_->SetUsedBytes(used_bytes_);
}

std::string DiskCacheManager::GetCachePath(const CacheKey& key) {
  return layout_->GetCachePath(key);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
