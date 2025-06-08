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

#include "cache/config/config.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

using dingofs::base::time::TimeNow;

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   DiskCacheLayoutSPtr layout)
    : running_(false),
      used_bytes_(0),
      capacity_bytes_(capacity),
      stage_full_(false),
      cache_full_(false),
      layout_(layout),
      cache_block_(std::make_unique<LRUCache>()),
      task_pool_(std::make_unique<TaskThreadPool>("disk_cache_manager")) {
  mq_ = std::make_unique<MessageQueueType>("delete_block_queue", 10);
  mq_->Subscribe([&](MessageType message) {
    DeleteBlocks(message.first, message.second);
  });
}

void DiskCacheManager::Start() {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Disk cache manager starting...";

    used_bytes_ = 0;  // For restart
    mq_->Start();
    CHECK_EQ(task_pool_->Start(2), 0);
    task_pool_->Enqueue(&DiskCacheManager::CheckFreeSpace, this);
    task_pool_->Enqueue(&DiskCacheManager::CleanupExpire, this);

    LOG(INFO) << absl::StrFormat(
        "Disk cache manager started, capacity = %.2lf MiB, "
        "free_space_ratio = %.2lf, cache_expire_s = %d",
        capacity_bytes_ * 1.0 / kMiB, FLAGS_free_space_ratio,
        FLAGS_cache_expire_s);
  }
}

void DiskCacheManager::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Disk cache manager stopping...";

    task_pool_->Stop();
    mq_->Stop();
    cache_block_->Clear();
    stage_block_.clear();

    LOG(INFO) << "Disk cache manager stopped.";
  }
}

void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value,
                           BlockPhase phase) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  if (phase == BlockPhase::kStaging) {
    stage_block_.emplace(key.Filename(), value);
    UpdateUsage(1, value.size);
  } else if (phase == BlockPhase::kUploaded) {
    auto iter = stage_block_.find(key.Filename());
    CHECK(iter != stage_block_.end());
    cache_block_->Add(key, iter->second);
    stage_block_.erase(iter);
  } else {  // cached
    cache_block_->Add(key, value);
    UpdateUsage(1, value.size);
  }

  if (used_bytes_ >= capacity_bytes_) {
    uint64_t goal_bytes = capacity_bytes_ * 0.95;
    uint64_t goal_files = cache_block_->Size() * 0.95;
    CleanupFull(goal_bytes, goal_files);
  }
}

void DiskCacheManager::Delete(const CacheKey& key) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  CacheValue value;
  if (cache_block_->Delete(key, &value)) {  // exist
    UpdateUsage(-1, -value.size);
  }
}

bool DiskCacheManager::Exist(const CacheKey& key) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  CacheValue value;
  return cache_block_->Get(key, &value) ||
         stage_block_.find(key.Filename()) != stage_block_.end();
}

bool DiskCacheManager::StageFull() const {
  return stage_full_.load(std::memory_order_acquire);
}

bool DiskCacheManager::CacheFull() const {
  return cache_full_.load(std::memory_order_acquire);
}

void DiskCacheManager::CheckFreeSpace() {
  struct FSStat stat;
  uint64_t goal_bytes, goal_files;
  std::string root_dir = GetRootDir();

  while (running_.load(std::memory_order_relaxed)) {
    auto status = Helper::StatFS(root_dir, &stat);
    if (!status.ok()) {
      LOG(ERROR) << "Check free space failed: " << status.ToString();
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

    if (cache_full) {
      double watermark = 1.0 - cfg;

      LOG_EVERY_SECOND(WARNING) << absl::StrFormat(
          "Disk usage is so high: dir = %s, watermark = %.2f%%, "
          "bytes_usage = %.2f%%, files_usage = %.2f%%, stop_cache = %c, "
          "stop_stage = %c.",
          root_dir, watermark * 100, (1.0 - br) * 100, (1.0 - fr) * 100,
          cache_full ? 'Y' : 'N', stage_full ? 'Y' : 'N');

      std::lock_guard<BthreadMutex> lk(mutex_);
      goal_bytes = stat.total_bytes * watermark;
      goal_files = stat.total_files * watermark;
      CleanupFull(goal_bytes, goal_files);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// protect by mutex
void DiskCacheManager::CleanupFull(uint64_t goal_bytes, uint64_t goal_files) {
  auto to_del = cache_block_->Evict([&](const CacheValue& value) {
    if (used_bytes_ <= goal_bytes && cache_block_->Size() <= goal_files) {
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
  CacheItems to_del;
  while (running_.load(std::memory_order_relaxed)) {
    uint64_t num_checks = 0;
    auto now = TimeNow();
    auto cache_expire_s = FLAGS_cache_expire_s;
    if (cache_expire_s == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    {
      std::lock_guard<BthreadMutex> lk(mutex_);
      to_del = cache_block_->Evict([&](const CacheValue& value) {
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

void DiskCacheManager::UpdateUsage(int64_t /*n*/, int64_t used_bytes) {
  used_bytes_ += used_bytes;
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
