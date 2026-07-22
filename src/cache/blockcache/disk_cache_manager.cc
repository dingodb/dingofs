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
#include <butil/memory/scope_guard.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <memory>

#include "cache/common/macro.h"
#include "cache/iutil/file_util.h"
#include "common/const.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {

DEFINE_double(free_space_ratio, 0.1,
              "cull watermark: start background eviction when the disk free "
              "space or inode ratio is below it");
DEFINE_validator(free_space_ratio, brpc::PassValidate);

DEFINE_double(free_space_run_ratio, 0.12,
              "run watermark: background eviction frees space until the disk "
              "free ratio is restored above it");
DEFINE_validator(free_space_run_ratio, brpc::PassValidate);

DEFINE_double(free_space_stop_ratio, 0.03,
              "stop watermark: reject caching new blocks when the disk free "
              "ratio is below it (last-resort backstop)");
DEFINE_validator(free_space_stop_ratio, brpc::PassValidate);

DEFINE_uint32(cache_expire_s, 259200,
              "expiration time for cache blocks in seconds");
DEFINE_validator(cache_expire_s, brpc::PassValidate);

DEFINE_uint32(cache_cleanup_expire_interval_ms, 1000,
              "interval for cleaning up expired cache blocks in milliseconds");
DEFINE_validator(cache_cleanup_expire_interval_ms, brpc::PassValidate);

static EvictionGuard::Config GuardConfig() {
  return {FLAGS_warmup_protect_s, FLAGS_warmup_protect_max_ratio};
}

static AdmissionController::Config AdmissionConfig() {
  return {FLAGS_cache_admit_second_hit, FLAGS_cache_admit_write_budget_mbps};
}

static uint64_t NowSec() { return iutil::TimeNow().sec; }

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   DiskCacheLayoutSPtr layout)
    : running_(false),
      capacity_bytes_(capacity),
      cached_blocks_(
          NewEvictionPolicy(FLAGS_cache_eviction_policy, capacity)),
      guard_(capacity),
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
  inflight_free_bytes_ = 0;
  inflight_free_files_ = 0;
  cached_blocks_->Clear();
  guard_.Reset();
  admission_.Reset();
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

  // stop == cull is allowed: it empties the cull band, collapsing the ladder
  // back to binary reject-at-cull — the rollback switch to pre-P0 behavior
  CHECK(FLAGS_free_space_stop_ratio <= FLAGS_free_space_ratio &&
        FLAGS_free_space_ratio <= FLAGS_free_space_run_ratio)
      << "Free space watermarks must satisfy stop <= cull <= run: "
      << FLAGS_free_space_stop_ratio << " / " << FLAGS_free_space_ratio
      << " / " << FLAGS_free_space_run_ratio;

  thread_pool_->Enqueue(&DiskCacheManager::CheckFreeSpace, this);
  thread_pool_->Enqueue(&DiskCacheManager::CleanupExpire, this);

  vars_->eviction_policy.set_value(FLAGS_cache_eviction_policy);

  LOG(INFO) << absl::StrFormat(
      "Disk cache manager is up: dir = %s, capacity = %.2lf MiB, "
      "free_space_ratio(run/cull/stop) = %.2lf/%.2lf/%.2lf, "
      "cache_expire_s = %lu",
      GetRootDir(), capacity_bytes_ * 1.0 / kMiB, FLAGS_free_space_run_ratio,
      FLAGS_free_space_ratio, FLAGS_free_space_stop_ratio,
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

bool DiskCacheManager::Admit(const CacheKey& key, BlockSource source,
                             size_t size) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  return admission_.Admit(AdmissionConfig(), NowSec(), key, source, size);
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
    CacheValue stamped = value;
    guard_.OnAdd(GuardConfig(), NowSec(), &stamped);
    cached_blocks_->Add(key, stamped);
    UpdateUsage(1, stamped.size);
  }

  if (used_bytes_ >= capacity_bytes_) {
    uint64_t want_free_bytes =
        used_bytes_ -
        (capacity_bytes_ * FreeSpaceMonitor::kCapacityEvictGoalRatio);
    uint64_t want_free_files =
        cached_blocks_->Size() * FreeSpaceMonitor::kCapacityEvictFilesRatio;
    LOG(INFO) << absl::StrFormat(
        "Trigger delete block for size reach capacity: "
        "used = %.2lf MiB, capacity = %.2lf MiB, "
        "want free %.2lf MiB %llu files",
        used_bytes_ * 1.0 / kMiB, capacity_bytes_ * 1.0 / kMiB,
        want_free_bytes * 1.0 / kMiB, want_free_files);
    CleanupFull(want_free_bytes, want_free_files, EvictReason::kCapacity);
  }
}

void DiskCacheManager::Delete(const CacheKey& key) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  CacheValue value;
  if (cached_blocks_->Delete(key, &value)) {  // exist
    guard_.OnRemove(value);
    UpdateUsage(-1, -value.size);
  }
}

void DiskCacheManager::Touch(const CacheKey& key) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  CacheValue value;
  cached_blocks_->Touch(key, &value);
}

// FIXME: lock contention
bool DiskCacheManager::Exist(const CacheKey& key) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  return cached_blocks_->Exist(key) ||
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

  using Band = FreeSpaceMonitor::Band;

  struct iutil::StatFS stat;
  std::string root_dir = GetRootDir();
  Band band = Band::kOk;

  while (running_.load(std::memory_order_relaxed)) {
    auto status = iutil::StatFS(root_dir, &stat);
    if (!status.ok()) {
      LOG(WARNING) << "Fail to check disk free space, dir=" << root_dir
                   << ", status=" << status.ToString()
                   << ", retry after 1 second...";
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    auto thresholds = FreeSpaceMonitor::Thresholds{FLAGS_free_space_run_ratio,
                                                   FLAGS_free_space_ratio,
                                                   FLAGS_free_space_stop_ratio}
                          .Sanitized();
    auto snapshot =
        FreeSpaceMonitor::StatFS{stat.free_bytes_ratio, stat.free_files_ratio,
                                 stat.total_bytes, stat.total_files};
    band = FreeSpaceMonitor::Judge(thresholds, snapshot, band);

    // only the stop band rejects new blocks; the cull band keeps admitting
    // while background eviction restores free space
    bool stop = (band == Band::kStop);
    cache_full_.store(stop, std::memory_order_release);
    stage_full_.store(stop, std::memory_order_release);
    vars_->cache_full.set_value(stop);
    vars_->stage_full.set_value(stop);
    vars_->free_space_band.set_value(static_cast<int64_t>(band));

    {
      std::lock_guard<bthread::Mutex> lk(mutex_);
      auto stats = cached_blocks_->GetStats();
      vars_->s3fifo_small_bytes.set_value(stats.small_bytes);
      vars_->ghost_entries.set_value(stats.ghost_entries);
      vars_->ghost_hits.set_value(stats.ghost_hits);
      vars_->protected_bytes.set_value(guard_.ProtectedBytes());
      vars_->protect_skips.set_value(guard_.ProtectSkips());
      vars_->evict_force_unprotect.set_value(guard_.ForceUnprotectRounds());
      vars_->admit_accepts.set_value(admission_.Accepts());
      vars_->admit_rejects_second_hit.set_value(
          admission_.RejectsSecondHit());
      vars_->admit_rejects_write_budget.set_value(
          admission_.RejectsWriteBudget());
    }

    if (band != Band::kOk) {
      LOG_EVERY_SECOND(WARNING) << absl::StrFormat(
          "Disk free space is under pressure: dir(%s), band(%s), "
          "free space%%(%.2lf), free inode%%(%.2lf), used(%.2lf MiB), "
          "stop(cache=%s,stage=%s)",
          root_dir, band == Band::kStop ? "STOP" : "CULL",
          stat.free_bytes_ratio * 100, stat.free_files_ratio * 100,
          used_bytes_ * 1.0 / kMiB, stop ? "Y" : "N", stop ? "Y" : "N");

      std::lock_guard<bthread::Mutex> lk(mutex_);
      auto target = FreeSpaceMonitor::CalcTarget(
          thresholds, snapshot,
          FreeSpaceMonitor::Usage{
              used_bytes_, capacity_bytes_, cached_blocks_->Size(),
              inflight_free_bytes_.load(std::memory_order_relaxed),
              inflight_free_files_.load(std::memory_order_relaxed)});
      if (target.NeedEvict()) {
        LOG(INFO) << absl::StrFormat(
            "Trigger delete block for disk full: "
            "used = %.2lf MiB, want free %.2lf MiB %llu files",
            used_bytes_ * 1.0 / kMiB, target.want_free_bytes * 1.0 / kMiB,
            target.want_free_files);
        CleanupFull(target.want_free_bytes, target.want_free_files,
                    EvictReason::kFreeSpace);
      }
    }

    // check faster while under pressure to shrink the stop-band window
    std::this_thread::sleep_for(std::chrono::milliseconds(
        band == Band::kOk ? 1000 : 200));
  }
}

// protected by mutex
void DiskCacheManager::CleanupFull(uint64_t want_free_bytes,
                                   uint64_t want_free_files,
                                   EvictReason reason) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  // per-round cap against eviction storms caused by watermark misjudgement
  const uint64_t max_evict_files =
      std::max<uint64_t>(1024, 2 * want_free_files);
  auto base = [&](const CacheValue& value) {
    if ((freed_bytes >= want_free_bytes && freed_files >= want_free_files) ||
        freed_files >= max_evict_files) {
      return FilterStatus::kFinish;
    }

    freed_bytes += value.size;
    freed_files++;
    guard_.OnRemove(value);
    UpdateUsage(-1, -value.size);
    return FilterStatus::kEvictIt;
  };
  auto to_del = cached_blocks_->Evict(guard_.Wrap(GuardConfig(), NowSec(), base));
  guard_.OnRoundEnd(/*under_pressure=*/true, freed_bytes);

  if (!to_del.empty()) {
    RecordEvicted(reason, freed_files, freed_bytes);
    SubmitDelete(std::move(to_del), reason);
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

    uint64_t freed_bytes = 0;
    {
      std::lock_guard<bthread::Mutex> lk(mutex_);
      to_del = cached_blocks_->Sweep([&](const CacheValue& value) {
        if (++num_checks > 1e3) {
          return FilterStatus::kFinish;
        } else if (value.atime + cache_expire_s > now) {
          return FilterStatus::kSkip;
        }
        freed_bytes += value.size;
        guard_.OnRemove(value);
        UpdateUsage(-1, -value.size);
        return FilterStatus::kEvictIt;
      });
    }

    if (!to_del.empty()) {
      RecordEvicted(EvictReason::kExpire, to_del.size(), freed_bytes);
      SubmitDelete(std::move(to_del), EvictReason::kExpire);
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_cache_cleanup_expire_interval_ms));
  }
}

static const char* ToString(DiskCacheManager::EvictReason reason) {
  switch (reason) {
    case DiskCacheManager::EvictReason::kCapacity:
      return "capacity full";
    case DiskCacheManager::EvictReason::kFreeSpace:
      return "disk full";
    case DiskCacheManager::EvictReason::kExpire:
      return "cache expired";
  }
  return "unknown";
}

void DiskCacheManager::SubmitDelete(CacheItems items, EvictReason reason) {
  uint64_t bytes = 0;
  for (const auto& item : items) {
    bytes += item.value.size;
  }
  inflight_free_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  inflight_free_files_.fetch_add(items.size(), std::memory_order_relaxed);
  CHECK_EQ(0, bthread::execution_queue_execute(
                  queue_id_, ToDel{std::move(items), ToString(reason)}));
}

void DiskCacheManager::RecordEvicted(EvictReason reason, uint64_t blocks,
                                     uint64_t bytes) {
  switch (reason) {
    case EvictReason::kCapacity:
      vars_->evict_blocks_capacity << blocks;
      vars_->evict_bytes_capacity << bytes;
      break;
    case EvictReason::kFreeSpace:
      vars_->evict_blocks_free_space << blocks;
      vars_->evict_bytes_free_space << bytes;
      break;
    case EvictReason::kExpire:
      vars_->evict_blocks_expire << blocks;
      vars_->evict_bytes_expire << bytes;
      break;
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

  // whatever the unlink outcome, these items are no longer in flight
  BRPC_SCOPE_EXIT {
    uint64_t bytes = 0;
    for (const auto& item : to_del.items) {
      bytes += item.value.size;
    }
    inflight_free_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
    inflight_free_files_.fetch_sub(to_del.items.size(),
                                   std::memory_order_relaxed);
  };

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
