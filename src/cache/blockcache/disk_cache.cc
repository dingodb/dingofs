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
#include <memory>

#include "cache/blockcache/disk_cache_manager.h"
#include "cache/blockcache/local_filesystem.h"
#include "cache/common/context.h"
#include "cache/iutil/file_util.h"
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

DiskCacheOption::DiskCacheOption()
    : cache_index(0),
      cache_store(FLAGS_cache_store),
      cache_dir(FLAGS_cache_dir),
      cache_size_mb(FLAGS_cache_size_mb) {}

DiskCache::DiskCache(DiskCacheOption option)
    : running_(false),
      option_(option),
      layout_(std::make_shared<DiskCacheLayout>(option.cache_index,
                                                option.cache_dir)),
      localfs_(std::make_unique<LocalFileSystem>(layout_)),
      manager_(std::make_shared<DiskCacheManager>(option.cache_size_mb * kMiB,
                                                  layout_)),
      loader_(std::make_unique<DiskCacheLoader>(layout_, manager_)),
      vars_(std::make_unique<DiskCacheVarsCollector>(
          option.cache_index, option.cache_dir, option.cache_size_mb,
          FLAGS_free_space_ratio)) {}

Status DiskCache::Start(UploadFunc uploader) {
  CHECK_NOTNULL(uploader);

  if (running_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is starting...";

  uploader_ = uploader;
  vars_->Reset();

  auto status = CreateDirs();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to create directories";
    return status;
  }

  status = LoadOrCreateLockFile();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to load or create lock file";
    return status;
  }

  status = localfs_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start LocalFileSystem";
    return status;
  }
  // Manage disk capacity, cache expire
  manager_->Start();

  // Load stage and cache block
  loader_->Start(uuid_, uploader);

  // TODO: Detect filesystem whether support direct IO

  running_ = true;
  vars_->uuid.set_value(uuid_);
  vars_->running_status.set_value("up");

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is up.";

  running_.store(true, std::memory_order_relaxed);
  return Status::OK();
}

Status DiskCache::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is shutting down...";

  // Metric: set running status to down
  vars_->running_status.set_value("down");

  // Shutdown load and manager
  loader_->Shutdown();
  manager_->Shutdown();

  // Shutdown filesystem
  auto status = localfs_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown LocalFileSystem";
    return status;
  }

  LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is down";

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
    auto status = iutil::MkDirs(dir);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to create directory=`" << dir << "'";
      return status;
    }
  }
  return Status::OK();
}

Status DiskCache::LoadOrCreateLockFile() {
  std::string content;
  auto lock_path = GetLockPath();
  auto status = iutil::ReadFile(lock_path, &content);
  if (status.ok()) {
    uuid_ = utils::TrimSpace(content);
  } else if (status.IsNotFound()) {
    uuid_ = utils::GenerateUUID();
    status = iutil::WriteFile(lock_path, uuid_);
  }

  if (!status.ok()) {
    LOG(ERROR) << "Fail to load or create lock file=" << lock_path;
    return status;
  } else if (uuid_.empty()) {
    LOG(ERROR) << "Load lock file success but the uuid in it is broken, file="
               << lock_path;
    return Status::Internal("invalid disk id");
  }

  return status;
}

Status DiskCache::Stage(ContextSPtr ctx, const BlockKey& key,
                        const Block& block, StageOption option) {
  Status status;
  DiskCacheVarsRecordGuard guard(__func__, status, vars_.get());

  status = CheckStatus(kWantExec | kWantStage);
  if (!status.ok()) {
    LOG(ERROR) << "Disk cache status is unavailable, skip stage: key="
               << key.Filename() << ", length=" << block.size
               << ", status=" << status.ToString();
    return status;
  }

  std::string stage_path(GetStagePath(key));
  std::string cache_path(GetCachePath(key));
  status = localfs_->WriteFile(ctx, stage_path, &block.buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to write stage file, path=" << stage_path
               << ", length=" << block.size << ", status=" << status.ToString();
    return status;
  }

  // FIXME: link error maybe cause:
  //   1) disk capacity managment inaccurate
  //   2) IO error: block which created by writeback will not founded
  status = iutil::Link(stage_path, cache_path);
  if (!status.ok()) {
    LOG(ERROR)
        << "Fail to link stage file to cache file, ignore error: stage_path="
        << stage_path << ", cache_path=" << cache_path
        << ", status=" << status.ToString();
    status = Status::OK();  // ignore link error
  }

  manager_->Add(key, CacheValue(block.size, iutil::TimeNow()),
                BlockPhase::kStaging);

  uploader_(NewContext(ctx->TraceId()), key, block.size, option.block_attr);
  return status;
}

Status DiskCache::RemoveStage(ContextSPtr /*ctx*/, const BlockKey& key,
                              RemoveStageOption /*option*/) {
  // NOTE: we will try to delete stage file even if the disk cache
  //       is down or unhealthy, so we remove the CheckStatus(...) here.
  // status = CheckStatus(...);

  auto stage_path = GetStagePath(key);
  auto status = iutil::Unlink(stage_path);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to remove stage file=`" << stage_path << "'";
  }

  manager_->Add(key, CacheValue(), BlockPhase::kUploaded);
  return status;
}

Status DiskCache::Cache(ContextSPtr ctx, const BlockKey& key,
                        const Block& block, CacheOption /*option*/) {
  auto status = CheckStatus(kWantExec | kWantCache);
  if (!status.ok()) {
    LOG(ERROR) << "Disk cache status is unavailable, skip cache: key="
               << key.Filename() << ", length=" << block.size
               << ", status=" << status.ToString();
    return status;
  }

  if (IsCached(key)) {
    VLOG(9) << "Block already cached, skip cache: key = " << key.Filename()
            << ", length = " << block.size;
    return Status::OK();
  }

  auto cache_path = GetCachePath(key);
  status = localfs_->WriteFile(ctx, cache_path, &block.buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to write cache file=`" << cache_path << "'";
    return status;
  }

  manager_->Add(key, CacheValue(block.size, iutil::TimeNow()),
                BlockPhase::kCached);

  return status;
}

Status DiskCache::Load(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer, LoadOption /*option*/) {
  Status status;
  DiskCacheVarsRecordGuard guard(__func__, status, vars_.get());

  status = CheckStatus(kWantExec);
  if (!status.ok()) {
    LOG(ERROR) << "Disk cache status is unavailable, skip load: key="
               << key.Filename() << ", offset=" << offset
               << ", length=" << length << ", status=" << status.ToString();
    return status;
  }

  if (!IsCached(key)) {
    status = Status::NotFound("cache not found");
    return status;
  }

  ctx->slice_id_ = key.id;
  auto cache_path = GetCachePath(key);
  status = localfs_->ReadFile(ctx, cache_path, offset, length, buffer);
  if (status.IsNotFound()) {  // Delete block which meybe deleted by accident.
    LOG(WARNING) << "Cache block file not found, delete the corresponding "
                    "key from lru, path=`"
                 << cache_path << "'";
    manager_->Delete(key);
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to read block file=`" << cache_path << "'";
  }

  return status;
}

// CheckStatus cache status:
//   1. check running status (UP/DOWN)
//   2. check disk healthy (HEALTHY/UNHEALTHY)
//   3. check disk free space (FULL or NOT)
Status DiskCache::CheckStatus(uint8_t want) const {
  if (!IsRunning()) {
    return Status::CacheDown("disk cache is down");
  }

  if ((want & kWantStage) && StageFull()) {
    return Status::CacheFull("disk cache is full");
  } else if ((want & kWantCache) && CacheFull()) {
    return Status::CacheFull("disk cache is full");
  }

  return Status::OK();
}

bool DiskCache::Dump(Json::Value& value) const {
  value["dir"] = option_.cache_dir;
  value["capacity"] = option_.cache_size_mb;
  value["free_space_ratio"] = FLAGS_free_space_ratio * 100;
  value["stage_full"] = StageFull();
  value["cache_full"] = CacheFull();

  return true;
}

}  // namespace cache
}  // namespace dingofs
