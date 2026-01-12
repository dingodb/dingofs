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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/disk_cache.h"

#include <absl/strings/str_format.h>
#include <fmt/format.h>

#include <atomic>

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/common/state_machine_impl.h"
#include "cache/metric/disk_cache_metric.h"
#include "cache/storage/base_filesystem.h"
#include "cache/storage/filesystem.h"
#include "cache/storage/hf3fs.h"
#include "cache/storage/local_filesystem.h"
#include "cache/utils/context.h"
#include "cache/utils/posix.h"
#include "common/const.h"
#include "common/helper.h"
#include "common/options/cache.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_dir, kDefaultCacheDir, "directory to store blocks");
DEFINE_validator(cache_dir, [](const char* /*name*/, const std::string& value) {
  FLAGS_cache_dir = dingofs::Helper::ExpandPath(value);
  return true;
});
DEFINE_string(cache_dir_uuid, "", "");
DEFINE_uint32(cache_size_mb, 102400, "maximum size of the cache in MB");

static const std::string kModule = "diskcache";

DiskCacheOption::DiskCacheOption()
    : cache_index(0),
      cache_store(FLAGS_cache_store),
      cache_dir(FLAGS_cache_dir),
      cache_size_mb(FLAGS_cache_size_mb) {}

DiskCache::DiskCache(DiskCacheOption option)
    : running_(false), support_direct_io_(false) {
  // metric
  metric_ = std::make_shared<DiskCacheMetric>(
      option.cache_index, option.cache_dir, option.cache_size_mb,
      FLAGS_free_space_ratio);

  // layout
  layout_ = std::make_shared<DiskCacheLayout>(option.cache_dir);

  // health checker
  state_machine_ = std::make_shared<StateMachineImpl>(kDiskStateMachine);
  disk_state_health_checker_ = std::make_unique<DiskStateHealthChecker>(
      metric_, layout_, state_machine_);

  // filesystem
  auto check_status_func = [&](Status status) {
    if (status.ok()) {
      state_machine_->Success();
    } else if (status.IsIoError()) {
      state_machine_->Error();
    }
    return status;
  };
  if (option.cache_store == "3fs") {
    fs_ = std::make_shared<HF3FS>(option.cache_dir, check_status_func);
    LOG(INFO) << "Using 3fs filesystem.";
  } else {
    fs_ = std::make_shared<LocalFileSystem>(check_status_func);
    LOG(INFO) << "Using local filesystem.";
  }

  // manager & loader
  manager_ = std::make_shared<DiskCacheManager>(option.cache_size_mb * kMiB,
                                                layout_, metric_);
  loader_ = std::make_unique<DiskCacheLoader>(layout_, manager_, metric_);
}

Status DiskCache::Start(UploadFunc uploader) {
  CHECK_NOTNULL(uploader);
  CHECK_NOTNULL(layout_);
  CHECK_NOTNULL(state_machine_);
  CHECK_NOTNULL(disk_state_health_checker_);
  CHECK_NOTNULL(fs_);
  CHECK_NOTNULL(metric_);
  CHECK_NOTNULL(manager_);
  CHECK_NOTNULL(loader_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is starting...";

  uploader_ = uploader;

  // Reset metric
  metric_->Reset();

  // Create directories
  auto status = CreateDirs();
  if (!status.ok()) {
    return status;
  }

  // Load disk id
  status = LoadOrCreateLockFile();
  if (!status.ok()) {
    return status;
  }

  // Start disk healther checker
  disk_state_health_checker_->Start();  // Probe disk health

  // Init filesystem which will perform IO operations
  status = fs_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start filesystem failed: " << status.ToString();
    return status;
  }

  // Start manager and loader
  manager_->Start();                // Manage disk capacity, cache expire
  loader_->Start(uuid_, uploader);  // Load stage and cache block

  // Detect filesystem whether support direct IO
  support_direct_io_ = DetectDirectIO();

  running_ = true;
  metric_->uuid.set_value(uuid_);
  metric_->running_status.set_value("up");

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is up.";

  CHECK_RUNNING("Disk cache");
  return Status::OK();
}

Status DiskCache::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is shutting down...";

  // Metric: set running status to down
  metric_->running_status.set_value("down");

  // Shutdown load and manager
  loader_->Shutdown();
  manager_->Shutdown();

  // Shutdown filesystem
  auto status = fs_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown filesystem failed: " << status.ToString();
    return status;
  }

  // Shutdown disk healther checker
  disk_state_health_checker_->Shutdown();

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is down.";

  CHECK_DOWN("Disk cache");
  return Status::OK();
}

Status DiskCache::CreateDirs() {
  std::vector<std::string> dirs{
      GetRootDir(),
      GetStageDir(),
      GetCacheDir(),
      GetProbeDir(),
  };
  for (const auto& dir : dirs) {
    auto status = FSUtil::MkDirs(dir);
    if (!status.ok()) {
      LOG(ERROR) << "Create directory failed: dir = " << dir
                 << ", status = " << status.ToString();
      return status;
    }
  }
  return Status::OK();
}

Status DiskCache::LoadOrCreateLockFile() {
  std::string content;
  auto lock_path = GetLockPath();
  auto status = FSUtil::ReadFile(lock_path, &content);
  if (status.ok()) {
    uuid_ = utils::TrimSpace(content);
  } else if (status.IsNotFound()) {
    uuid_ = utils::GenerateUUID();
    status = FSUtil::WriteFile(lock_path, uuid_);
  }

  if (!status.ok()) {
    LOG(ERROR) << "Load or create lock file failed: path = " << lock_path
               << ", status = " << status.ToString();
    return status;
  } else if (uuid_.empty()) {
    LOG(ERROR) << "Load disk uuid success but is invalid: path = " << lock_path;
    return Status::Internal("invalid disk id");
  }

  return status;
}

// Detect filesystem whether support direct IO, filesystem
// like tmpfs (/dev/shm) will not support it.
bool DiskCache::DetectDirectIO() {
  int fd;
  int flags = Posix::kDefaultCreatFlags | O_DIRECT;
  auto filepath = GetDetectPath();
  auto status = Posix::Open(filepath, flags, 0644, &fd);
  Posix::Close(fd);
  Posix::Unlink(filepath);

  if (status.ok()) {
    LOG(INFO) << "The filesystem of disk cache (dir=" << GetRootDir()
              << ") supports direct IO.";
    return true;
  }

  LOG(INFO) << "The filesystem of disk cache (dir=" << GetRootDir()
            << ") not support direct IO, using buffer IO, detect rc = "
            << status.ToString();
  return false;
}

Status DiskCache::Stage(ContextSPtr ctx, const BlockKey& key,
                        const Block& block, StageOption option) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "stage(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);
  DiskCacheMetricGuard metric_guard(__func__, status, metric_);

  status = CheckStatus(kWantExec | kWantStage);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Disk cache status is unavailable, skip stage: key = "
                   << key.Filename() << ", length = " << block.size
                   << ", status = " << status.ToString();
    return status;
  }

  NEXT_STEP("write");
  std::string stage_path(GetStagePath(key));
  std::string cache_path(GetCachePath(key));
  status = fs_->WriteFile(ctx, stage_path, block.buffer,
                          WriteOption{.direct_io = support_direct_io_});
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Write stage block file failed: path = " << stage_path
                   << ", length = " << block.size
                   << ", status = " << status.ToString();
    return status;
  }

  // FIXME: link error maybe cause:
  //   1) disk capacity managment inaccurate
  //   2) IO error: block which created by writeback will not founded
  NEXT_STEP("link");
  status = fs_->Link(stage_path, cache_path);
  if (!status.ok()) {
    LOG_CTX(ERROR)
        << "Link stage file to cache file failed, ignore error: stage_path = "
        << stage_path << ", cache_path = " << cache_path
        << ", status = " << status.ToString();
    status = Status::OK();  // ignore link error
  }

  NEXT_STEP("cache_add");
  manager_->Add(key, CacheValue(block.size, utils::TimeNow()),
                BlockPhase::kStaging);

  NEXT_STEP("enqueue");
  uploader_(NewContext(ctx->TraceId()), key, block.size, option.block_ctx);

  return status;
}

Status DiskCache::RemoveStage(ContextSPtr ctx, const BlockKey& key,
                              RemoveStageOption /*option*/) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "removestage(%s)",
                    key.Filename());
  StepTimerGuard guard(timer);

  // NOTE: we will try to delete stage file even if the disk cache
  //       is down or unhealthy, so we remove the CheckStatus(...) here.
  // status = CheckStatus(...);

  NEXT_STEP("unlink");
  auto stage_path = GetStagePath(key);
  status = fs_->RemoveFile(stage_path);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Remove stage block file failed: path = " << stage_path
                   << ", status = " << status.ToString();
  }

  NEXT_STEP("cache_add");
  manager_->Add(key, CacheValue(), BlockPhase::kUploaded);

  return status;
}

Status DiskCache::Cache(ContextSPtr ctx, const BlockKey& key,
                        const Block& block, CacheOption /*option*/) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "cache(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);

  status = CheckStatus(kWantExec | kWantCache);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Disk cache status is unavailable, skip cache: key = "
                   << key.Filename() << ", length = " << block.size
                   << ", status = " << status.ToString();
    return status;
  }

  if (IsCached(key)) {
    VLOG_CTX(9) << "Block already cached, skip cache: key = " << key.Filename()
                << ", length = " << block.size;
    return Status::OK();
  }

  NEXT_STEP("write");
  auto cache_path = GetCachePath(key);
  status = fs_->WriteFile(ctx, cache_path, block.buffer,
                          WriteOption{.direct_io = support_direct_io_});
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Write cache block file failed: path = " << cache_path
                   << ", length = " << block.size
                   << ", status = " << status.ToString();
    return status;
  }

  NEXT_STEP("cache_add");
  manager_->Add(key, CacheValue(block.size, utils::TimeNow()),
                BlockPhase::kCached);

  return status;
}

Status DiskCache::Load(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer, LoadOption /*option*/) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "load(%s,%zu,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);
  DiskCacheMetricGuard metric_guard(__func__, status, metric_);

  status = CheckStatus(kWantExec);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Disk cache status is unavailable, skip load: key = "
                   << key.Filename() << ", offset = " << offset
                   << ", length = " << length
                   << ", status = " << status.ToString();
    return status;
  }

  if (!IsCached(key)) {
    status = Status::NotFound("cache not found");
    return status;
  }

  NEXT_STEP("read");
  auto cache_path = GetCachePath(key);
  status = fs_->ReadFile(ctx, cache_path, offset, length, buffer,
                         ReadOption{.drop_page_cache = true});
  if (status.IsNotFound()) {  // Delete block which meybe deleted by accident.
    LOG_CTX(WARNING) << "Cache block file not found, delete the corresponding "
                        "key from lru: path = "
                     << cache_path;
    manager_->Delete(key);
  } else if (!status.ok()) {
    LOG_CTX(ERROR) << "Read cache block file failed: path = " << cache_path
                   << ", offset = " << offset << ", length = " << length
                   << ", status = " << status.ToString();
  }

  return status;
}

std::string DiskCache::Id() const { return uuid_; }

bool DiskCache::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

bool DiskCache::IsCached(const BlockKey& key) const {
  std::string cache_path = GetCachePath(key);
  if (manager_->Exist(key)) {
    return true;
  } else if (IsLoading() && fs_->FileExists(cache_path)) {
    return true;
  }
  return false;
}

// CheckStatus cache status:
//   1. check running status (UP/DOWN)
//   2. check disk healthy (HEALTHY/UNHEALTHY)
//   3. check disk free space (FULL or NOT)
Status DiskCache::CheckStatus(uint8_t want) const {
  if (!IsRunning()) {
    return Status::CacheDown("disk cache is down");
  }

  if ((want & kWantExec) && !IsHealthy()) {
    return Status::CacheUnhealthy("disk cache is unhealthy");
  } else if ((want & kWantStage) && StageFull()) {
    return Status::CacheFull("disk cache is full");
  } else if ((want & kWantCache) && CacheFull()) {
    return Status::CacheFull("disk cache is full");
  }

  return Status::OK();
}

// disk cache status
bool DiskCache::IsLoading() const { return loader_->IsLoading(); }

bool DiskCache::IsHealthy() const {
  return state_machine_->GetState() == State::kStateNormal;
}

bool DiskCache::StageFull() const { return manager_->StageFull(); }
bool DiskCache::CacheFull() const { return manager_->CacheFull(); }

// utility
std::string DiskCache::GetRootDir() const { return layout_->GetRootDir(); }
std::string DiskCache::GetStageDir() const { return layout_->GetStageDir(); }
std::string DiskCache::GetCacheDir() const { return layout_->GetCacheDir(); }
std::string DiskCache::GetProbeDir() const { return layout_->GetProbeDir(); }

std::string DiskCache::GetDetectPath() const {
  return layout_->GetDetectPath();
}

std::string DiskCache::GetLockPath() const { return layout_->GetLockPath(); }

std::string DiskCache::GetStagePath(const BlockKey& key) const {
  return layout_->GetStagePath(key);
}

std::string DiskCache::GetCachePath(const BlockKey& key) const {
  return layout_->GetCachePath(key);
}

}  // namespace cache
}  // namespace dingofs
