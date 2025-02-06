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

#include <glog/logging.h>
#include <sys/mman.h>

#include <memory>

#include "absl/cleanup/cleanup.h"
#include "base/math/math.h"
#include "base/string/string.h"
#include "base/time/time.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "cache/blockcache/disk_cache_metric.h"
#include "cache/common/errno.h"
#include "cache/common/local_filesystem.h"
#include "cache/common/log.h"
#include "cache/common/phase_timer.h"
#include "cache/common/posix.h"
#include "stub/metric/metric.h"
#include "utils/dingo_define.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using base::math::kMiB;
using base::string::GenUuid;
using base::string::TrimSpace;
using base::time::TimeNow;
using cache::common::LocalFileSystem;
using cache::common::LogGuard;
using cache::common::Phase;
using cache::common::PhaseTimer;
using cache::common::Posix;

using DiskCacheTotalMetric = ::dingofs::stub::metric::DiskCacheMetric;
using DiskCacheMetricGuard =
    ::dingofs::client::blockcache::DiskCacheMetricGuard;

BlockReaderImpl::BlockReaderImpl(int fd, std::shared_ptr<LocalFileSystem> fs)
    : fd_(fd), fs_(fs) {}

Errno BlockReaderImpl::ReadAt(off_t offset, size_t length, char* buffer) {
  return fs_->Do([&]() {
    Errno rc;
    DiskCacheMetricGuard guard(
        &rc, &DiskCacheTotalMetric::GetInstance().read_disk, length);
    rc = Posix::LSeek(fd_, offset, SEEK_SET);
    if (rc == Errno::OK) {
      rc = Posix::Read(fd_, buffer, length);
    }
    return rc;
  });
}

void BlockReaderImpl::Close() {
  fs_->Do([&]() {
    Posix::Close(fd_);
    return Errno::OK;
  });
}

DiskCache::DiskCache(DiskCacheOptions options)
    : options_(options), running_(false), use_direct_write_(false) {
  metric_ = std::make_shared<DiskCacheMetric>(options);
  layout_ = std::make_shared<DiskCacheLayout>(options.cache_dir);
  disk_state_machine_ = std::make_shared<DiskStateMachineImpl>(metric_);
  disk_state_health_checker_ =
      std::make_unique<DiskStateHealthChecker>(layout_, disk_state_machine_);
  fs_ = std::make_shared<LocalFileSystem>(disk_state_machine_);
  manager_ = std::make_shared<DiskCacheManager>(options.cache_size_mb * kMiB,
                                                layout_, fs_, metric_);
  loader_ = std::make_unique<DiskCacheLoader>(layout_, fs_, manager_, metric_);
}

Errno DiskCache::Init(UploadFunc uploader) {
  if (running_.exchange(true)) {
    return Errno::OK;  // already running
  }

  auto rc = CreateDirs();
  if (rc == Errno::OK) {
    rc = LoadLockFile();
  }
  if (rc != Errno::OK) {
    return rc;
  }

  uploader_ = uploader;
  metric_->Init();   // For restart
  DetectDirectIO();  // Detect filesystem whether support direct IO, filesystem
                     // like tmpfs (/dev/shm) will not support it.
  disk_state_machine_->Start();         // monitor disk state
  disk_state_health_checker_->Start();  // probe disk health
  manager_->Start();                    // manage disk capacity, cache expire
  loader_->Start(uuid_, uploader);      // load stage and cache block
  metric_->SetUuid(uuid_);
  metric_->SetRunningStatus(kCacheUp);

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is up.";
  return Errno::OK;
}

Errno DiskCache::Shutdown() {
  if (!running_.exchange(false)) {
    return Errno::OK;
  }

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is shutting down...";

  loader_->Stop();
  manager_->Stop();
  disk_state_health_checker_->Stop();
  disk_state_machine_->Stop();
  metric_->SetRunningStatus(kCacheDown);

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is down.";
  return Errno::OK;
}

Errno DiskCache::Stage(const BlockKey& key, const Block& block,
                       BlockContext ctx) {
  Errno rc;
  PhaseTimer timer;
  auto metric_guard = absl::MakeCleanup([&] {
    if (rc == Errno::OK) {
      metric_->AddStageBlock(1);
    } else {
      metric_->AddStageSkip();
    }
  });
  LogGuard log([&]() {
    return StrFormat("stage(%s,%d): %s%s", key.Filename(), block.size,
                     StrErr(rc), timer.ToString());
  });

  rc = Check(WANT_EXEC | WANT_STAGE);
  if (rc != Errno::OK) {
    return rc;
  }

  timer.NextPhase(Phase::WRITE_FILE);
  std::string stage_path(GetStagePath(key));
  std::string cache_path(GetCachePath(key));
  rc = fs_->WriteFile(stage_path, block.data, block.size, use_direct_write_);
  if (rc != Errno::OK) {
    return rc;
  }

  timer.NextPhase(Phase::LINK);
  rc = fs_->HardLink(stage_path, cache_path);
  if (rc == Errno::OK) {
    timer.NextPhase(Phase::CACHE_ADD);
    manager_->Add(key, CacheValue(block.size, TimeNow()));
  } else {
    LOG(WARNING) << "Link " << stage_path << " to " << cache_path
                 << " failed: " << StrErr(rc);
    rc = Errno::OK;  // ignore link error
  }

  timer.NextPhase(Phase::ENQUEUE_UPLOAD);
  uploader_(key, stage_path, ctx);
  return rc;
}

Errno DiskCache::RemoveStage(const BlockKey& key, BlockContext ctx) {
  Errno rc;
  auto metric_guard = ::absl::MakeCleanup([&] {
    if (rc == Errno::OK) {
      metric_->AddStageBlock(-1);
    }
  });
  LogGuard log([&]() {
    return StrFormat("removestage(%s): %s", key.Filename(), StrErr(rc));
  });

  // NOTE: we will try to delete stage file even if the disk cache
  //       is down or unhealthy, so we remove the Check(...) here.
  rc = fs_->RemoveFile(GetStagePath(key));
  return rc;
}

Errno DiskCache::Cache(const BlockKey& key, const Block& block) {
  Errno rc;
  PhaseTimer timer;
  LogGuard log([&]() {
    return StrFormat("cache(%s,%d): %s%s", key.Filename(), block.size,
                     StrErr(rc), timer.ToString());
  });

  rc = Check(WANT_EXEC | WANT_CACHE);
  if (rc != Errno::OK) {
    return rc;
  }

  timer.NextPhase(Phase::WRITE_FILE);
  rc = fs_->WriteFile(GetCachePath(key), block.data, block.size);
  if (rc != Errno::OK) {
    return rc;
  }

  timer.NextPhase(Phase::CACHE_ADD);
  manager_->Add(key, CacheValue(block.size, TimeNow()));
  return rc;
}

Errno DiskCache::Load(const BlockKey& key,
                      std::shared_ptr<BlockReader>& reader) {
  Errno rc;
  PhaseTimer timer;
  auto metric_guard = absl::MakeCleanup([&] {
    if (rc == Errno::OK) {
      metric_->AddCacheHit();
    } else {
      metric_->AddCacheMiss();
    }
  });
  LogGuard log([&]() {
    return StrFormat("load(%s): %s%s", key.Filename(), StrErr(rc),
                     timer.ToString());
  });

  rc = Check(WANT_EXEC);
  if (rc != Errno::OK) {
    return rc;
  } else if (!IsCached(key)) {
    return Errno::NOT_FOUND;
  }

  timer.NextPhase(Phase::OPEN_FILE);
  rc = fs_->Do([&]() {
    int fd;
    auto rc = Posix::Open(GetCachePath(key), O_RDONLY, 0644, &fd);
    if (rc == Errno::OK) {
      reader = std::make_shared<BlockReaderImpl>(fd, fs_);
    }
    return rc;
  });

  // Delete corresponding key of block which maybe already deleted by accident.
  if (rc == Errno::NOT_FOUND) {
    manager_->Delete(key);
  }
  return rc;
}

Errno DiskCache::Load(const BlockKey& key, uint64_t offset, uint64_t size,
                      IOBuffer* buffer) {
  CHECK(offset % IO_ALIGNED_BLOCK_SIZE == 0);
  /*
    int fd;
    void* addr_out;

    std::string cache_path = GetCachePath(key);
    int rc = Posix::Open(cache_path, &fd);
    if (rc == Errno::OK) {
      rc = Posix::MMap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset,
                       &addr_out);
    }

    if (rc != Errno::OK) {
      return rc;
    }

    auto deleter = [this, fd, offset, length](void* ptr) {
      int rc = ::munmap(static_cast<char*>(ptr), length);
      if (rc != 0) {
        LOG(ERROR) << "munmap() failed: " << strerror(errno);
      }
      page_cache_manager_->DropPageCache(fd, offset, length);
    };

    *buffer = MMapBufer(addr_out, length, deleter);
    return rc;
  */
  return Errno::OK;
}

bool DiskCache::IsCached(const BlockKey& key) {
  CacheValue value;
  std::string cache_path = GetCachePath(key);
  auto rc = manager_->Get(key, &value);
  if (rc == Errno::OK) {
    return true;
  } else if (loader_->IsLoading() && fs_->FileExists(cache_path)) {
    return true;
  }
  return false;
}

std::string DiskCache::Id() { return uuid_; }

Errno DiskCache::CreateDirs() {
  std::vector<std::string> dirs{
      layout_->GetRootDir(),
      layout_->GetStageDir(),
      layout_->GetCacheDir(),
      layout_->GetProbeDir(),
  };

  for (const auto& dir : dirs) {
    auto rc = fs_->MkDirs(dir);
    if (rc != Errno::OK) {
      return rc;
    }
  }
  return Errno::OK;
}

Errno DiskCache::LoadLockFile() {
  size_t length;
  std::shared_ptr<char> buffer;
  auto lock_path = layout_->GetLockPath();
  auto rc = fs_->ReadFile(lock_path, buffer, &length);
  if (rc == Errno::OK) {
    uuid_ = TrimSpace(std::string(buffer.get(), length));
  } else if (rc == Errno::NOT_FOUND) {
    uuid_ = GenUuid();
    rc = fs_->WriteFile(lock_path, uuid_.c_str(), uuid_.size());
  }
  return rc;
}

void DiskCache::DetectDirectIO() {
  std::string filepath = layout_->GetDetectPath();
  auto rc = fs_->Do([filepath]() {
    int fd;
    auto rc = Posix::Create(filepath, &fd, true);
    Posix::Close(fd);
    Posix::Unlink(filepath);
    return rc;
  });

  if (rc == Errno::OK) {
    use_direct_write_ = true;
    LOG(INFO) << "The filesystem of disk cache (dir=" << layout_->GetRootDir()
              << ") supports direct IO.";
  } else {
    use_direct_write_ = false;
    LOG(INFO) << "The filesystem of disk cache (dir=" << layout_->GetRootDir()
              << ") not support direct IO, using buffer IO, detect rc = " << rc;
  }
  metric_->SetUseDirectWrite(use_direct_write_);
}

// Check cache status:
//   1. check running status (UP/DOWN)
//   2. check disk healthy (HEALTHY/UNHEALTHY)
//   3. check disk free space (FULL or NOT)
Errno DiskCache::Check(uint8_t want) {
  if (!running_.load(std::memory_order_relaxed)) {
    return Errno::CACHE_DOWN;
  }

  if ((want & WANT_EXEC) && !IsHealthy()) {
    return Errno::CACHE_UNHEALTHY;
  } else if ((want & WANT_STAGE) && StageFull()) {
    return Errno::CACHE_FULL;
  } else if ((want & WANT_CACHE) && CacheFull()) {
    return Errno::CACHE_FULL;
  }
  return Errno::OK;
}

bool DiskCache::IsLoading() const { return loader_->IsLoading(); }

bool DiskCache::IsHealthy() const {
  return disk_state_machine_->GetDiskState() == DiskState::kDiskStateNormal;
}

bool DiskCache::StageFull() const { return manager_->StageFull(); }

bool DiskCache::CacheFull() const { return manager_->CacheFull(); }

std::string DiskCache::GetRootDir() const { return layout_->GetRootDir(); }

std::string DiskCache::GetStagePath(const BlockKey& key) const {
  return layout_->GetStagePath(key);
}

std::string DiskCache::GetCachePath(const BlockKey& key) const {
  return layout_->GetCachePath(key);
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
