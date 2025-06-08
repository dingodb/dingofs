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

#include "cache/storage/filesystem.h"
#include "cache/storage/hf3fs.h"
#include "cache/storage/local_filesystem.h"
#include "cache/utils/access_log.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"
#include "cache/utils/state_machine_impl.h"

namespace dingofs {
namespace cache {

using dingofs::base::string::GenUuid;
using dingofs::base::string::TrimSpace;
using dingofs::base::time::TimeNow;

DiskCache::DiskCache(DiskCacheOption option) : running_(false) {
  // layout
  layout_ = std::make_shared<DiskCacheLayout>(option.cache_dir);

  // health checker
  state_machine_ = std::make_shared<StateMachineImpl>();
  disk_state_health_checker_ =
      std::make_unique<DiskStateHealthChecker>(layout_, state_machine_);

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
  } else {
    fs_ = std::make_shared<LocalFileSystem>(check_status_func);
  }

  // manager & loader
  manager_ =
      std::make_shared<DiskCacheManager>(option.cache_size_mb * kMiB, layout_);
  loader_ = std::make_unique<DiskCacheLoader>(layout_, manager_);
}

Status DiskCache::Init(UploadFunc uploader) {
  if (!running_.exchange(true, std::memory_order_acq_rel)) {
    LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") starting...";

    uploader_ = uploader;

    // create directories
    auto status = CreateDirs();
    if (!status.ok()) {
      return status;
    }

    // load disk id
    status = LoadOrCreateLockFile();
    if (!status.ok()) {
      return status;
    }

    // start disk healther checker
    disk_state_health_checker_->Start();  // probe disk health

    // init filesystem which will perform IO operations
    status = fs_->Init();
    if (!status.ok()) {
      return status;
    }

    // start manager and loader
    manager_->Start();                // manage disk capacity, cache expire
    loader_->Start(uuid_, uploader);  // load stage and cache block

    LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is up.";
  }

  return Status::OK();
}

Status DiskCache::Shutdown() {
  if (running_.exchange(false, std::memory_order_acq_rel)) {
    LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is shutting down...";

    // stop manager and loader
    loader_->Stop();
    manager_->Stop();

    // destroy filesystem
    auto status = fs_->Destroy();
    if (!status.ok()) {
      return status;
    }

    // stop disk healther checker
    disk_state_health_checker_->Stop();

    LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is down.";
  }

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
    auto status = Helper::MkDirs(dir);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status DiskCache::LoadOrCreateLockFile() {
  std::string content;
  auto lock_path = GetLockPath();
  auto status = Helper::ReadFile(lock_path, &content);
  if (status.ok()) {
    uuid_ = TrimSpace(content);
  } else if (status.IsNotFound()) {
    uuid_ = GenUuid();
    status = Helper::WriteFile(lock_path, uuid_);
  }
  return status;
}

// Detect filesystem whether support direct IO, filesystem
// like tmpfs (/dev/shm) will not support it.
bool DiskCache::DetectDirectIO() {
  int fd;
  int flags = O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT;
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

Status DiskCache::Stage(const BlockKey& key, const Block& block,
                        StageOption option) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[disk] stage(%s,%zu): %s%s", key.Filename(),
                           block.size, status.ToString(), timer.ToString());
  });

  status = Check(kWantExec | kWantStage);
  if (!status.ok()) {
    return status;
  }

  timer.NextPhase(Phase::kWriteFile);
  std::string stage_path(GetStagePath(key));
  std::string cache_path(GetCachePath(key));
  status = fs_->WriteFile(stage_path, block.buffer, WriteOption(true));
  if (!status.ok()) {
    return status;
  }

  // FIXME: link error maybe cause:
  //   1) disk capacity managment inaccurate
  //   2) io error: block which created by writeback will not founded
  timer.NextPhase(Phase::kLinkFile);
  status = fs_->Link(stage_path, cache_path);
  if (!status.ok()) {
    LOG(WARNING) << "Link " << stage_path << " to " << cache_path
                 << " failed: " << status.ToString();
    status = Status::OK();  // ignore link error
  }

  timer.NextPhase(Phase::kCacheAdd);
  manager_->Add(key, CacheValue(block.size, TimeNow()), BlockPhase::kStaging);

  timer.NextPhase(Phase::kEnterUploadQueue);
  uploader_(key, block.size, option.ctx);
  return status;
}

Status DiskCache::RemoveStage(const BlockKey& key,
                              RemoveStageOption /*option*/) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[disk] removestage(%s): %s", key.Filename(),
                           status.ToString());
  });

  // NOTE: we will try to delete stage file even if the disk cache
  //       is down or unhealthy, so we remove the Check(...) here.
  // status = Check(...);

  timer.NextPhase(Phase::kRemoveFile);
  status = fs_->RemoveFile(GetStagePath(key));

  manager_->Add(key, CacheValue(), BlockPhase::kUploaded);
  return status;
}

Status DiskCache::Cache(const BlockKey& key, const Block& block,
                        CacheOption /*option*/) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[disk] cache(%s,%zu): %s%s", key.Filename(),
                           block.size, status.ToString(), timer.ToString());
  });

  status = Check(kWantExec | kWantCache);
  if (!status.ok()) {
    return status;
  } else if (IsCached(key)) {
    return Status::OK();
  }

  timer.NextPhase(Phase::kWriteFile);
  status = fs_->WriteFile(GetCachePath(key), block.buffer, WriteOption(true));
  if (!status.ok()) {
    return status;
  }

  timer.NextPhase(Phase::kCacheAdd);
  manager_->Add(key, CacheValue(block.size, TimeNow()), BlockPhase::kCached);
  return status;
}

Status DiskCache::Load(const BlockKey& key, off_t offset, size_t length,
                       IOBuffer* buffer, LoadOption /*option*/) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[disk] load(%s,%lld,%zu): %s%s", key.Filename(),
                           offset, length, status.ToString(), timer.ToString());
  });

  status = Check(kWantExec);
  if (!status.ok()) {
    return status;
  } else if (!IsCached(key)) {
    return Status::NotFound("cache not found");
  }

  timer.NextPhase(Phase::kReadFile);
  status = fs_->ReadFile(GetCachePath(key), offset, length, buffer);

  // Delete the block which maybe already deleted by accident.
  if (status.IsNotFound()) {
    manager_->Delete(key);
  }

  return status;
}

std::string DiskCache::Id() const { return uuid_; }

bool DiskCache::IsRunning() const {
  return running_.load(std::memory_order_acquire);
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

// Check cache status:
//   1. check running status (UP/DOWN)
//   2. check disk healthy (HEALTHY/UNHEALTHY)
//   3. check disk free space (FULL or NOT)
Status DiskCache::Check(uint8_t want) const {
  if (!running_.load(std::memory_order_acquire)) {
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
