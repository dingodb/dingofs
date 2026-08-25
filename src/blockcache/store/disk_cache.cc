/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/store/disk_cache.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/utils/align.h"
#include "blockcache/utils/string.h"
#include "blockcache/utils/time.h"
#include "utils/uuid.h"

namespace dingofs {
namespace blockcache {

DEFINE_string(cache_dir, "/tmp/dingofs-cache",
              "directory list for stage and cached blocks; use comma-separated "
              "path[:size_mb] entries, e.g. /mnt/cache1:1024,/mnt/cache2");
DEFINE_string(cache_dir_uuid, "",
              "uuid suffix appended to each cache directory; set internally");
DEFINE_uint32(cache_size_mb, 102400, "cache size in mb");

std::vector<DiskOption> ParseDiskOptions(const std::string& value) {
  std::vector<DiskOption> options;
  for (std::string& item : Split(value, ',')) {
    DiskOption option;

    option.capacity_bytes = static_cast<uint64_t>(FLAGS_cache_size_mb) * kMiB;
    const size_t colon = item.find(':');
    if (colon != std::string::npos) {
      uint64_t size_mb = 0;
      std::string_view size_sv = std::string_view(item).substr(colon + 1);
      CHECK(SplitUint(&size_sv, '\0', &size_mb) && size_mb > 0)
          << "bad disk size in cache_dir item: " << item;
      option.capacity_bytes = size_mb * kMiB;
      item.resize(colon);
    }

    option.dir = std::move(item);
    if (!FLAGS_cache_dir_uuid.empty()) {
      option.dir += "/" + FLAGS_cache_dir_uuid;
    }

    options.push_back(std::move(option));
  }
  CHECK(!options.empty()) << "no cache dirs in: " << value;
  return options;
}

DiskCache::DiskCache(DiskOption option)
    : option_(std::move(option)),
      layout_(option_.dir),
      health_(std::make_unique<HealthChecker>(layout_)),
      manager_(std::make_unique<CacheManager>(
          layout_, option_.capacity_bytes / ShardCount())),
      loader_(std::make_unique<DiskCacheLoader>(layout_, manager_.get())),
      localfs_(std::make_unique<LocalFileSystem>(health_.get())) {}

Future<> DiskCache::Start(UploadFunc uploader) {
  CHECK(!running_) << "disk cache already started";
  CHECK(uploader != nullptr) << "a disk cache needs somewhere to send staged "
                                "blocks";

  running_ = true;

  LOG(INFO) << "DiskCache{dir=" << option_.dir << "} is starting...";

  Status status = CreateDirs();
  CHECK(status.ok()) << "Fail to prepare cache dir=`" << option_.dir
                     << "': " << status.ToString();

  status = GetOrCreateLockFile();
  CHECK(status.ok()) << "Fail to load or create lock file of cache dir=`"
                     << option_.dir << "': " << status.ToString();

  (void)loader_->Start(std::move(uploader));
  (void)manager_->Start();
  (void)health_->Start();

  LOG(INFO) << "Successfully start DiskCache: dir=" << option_.dir
            << " uuid=" << uuid_ << " shard=" << ThisShardId() << "/"
            << ShardCount()
            << " capacity_mb=" << (option_.capacity_bytes / kMiB);
  co_return;
}

Future<> DiskCache::Shutdown() {
  LOG(INFO) << "DiskCache{dir=" << option_.dir << "} is shutting down...";

  running_ = false;
  co_await loader_->Shutdown();
  co_await manager_->Shutdown();
  co_await health_->Shutdown();

  LOG(INFO) << "Successfully shutdown DiskCache{dir=" << option_.dir << "}";
}

Future<Status> DiskCache::Stage(BlockHandle handle, BufferViews block) {
  if (manager_->Exist(handle)) {
    co_return Status::OK();
  }

  const Status check = Check(kWantExec | kWantStage);
  if (!check.ok()) {
    co_return check;
  }

  const Status status =
      co_await localfs_->WriteFile(GetStagePath(handle), block);
  if (!status.ok()) {
    co_return status;
  }

  const Status linked =
      co_await localfs_->Link(GetStagePath(handle), GetCachePath(handle));
  LOG_IF(ERROR, !linked.ok())
      << "Fail to link stage file to cache file, ignore error: stage_path="
      << GetStagePath(handle) << ", cache_path=" << GetCachePath(handle)
      << ", status=" << linked.ToString();

  manager_->Insert(handle, TimestampSec(), true);
  co_return Status::OK();
}

Future<Status> DiskCache::RemoveStage(BlockHandle handle) {
  const std::optional<CacheManager::Entry> entry = manager_->Find(handle);
  if (!entry) {
    co_return Status::NotFound("no such block");
  } else if (!entry->staged) {
    co_return Status::OK();
  }

  const Status status = co_await localfs_->Unlink(GetStagePath(entry->handle));
  if (!status.ok() && !status.IsNotExist()) {  // unlink failed
    co_return status;
  }

  manager_->EraseStage(handle);
  co_return Status::OK();
}

Future<Status> DiskCache::Cache(BlockHandle handle, BufferViews block) {
  if (manager_->Exist(handle)) {
    co_return Status::OK();
  }

  const Status check = Check(kWantExec | kWantCache);
  if (!check.ok()) {
    co_return check;
  }

  const Status status =
      co_await localfs_->WriteFile(GetCachePath(handle), block);
  if (!status.ok()) {
    co_return status;
  }

  manager_->Insert(handle, TimestampSec(), false);
  co_return Status::OK();
}

Future<Status> DiskCache::Load(BlockHandle handle, uint64_t offset,
                               uint32_t length, char* buffer) {
  CHECK_LE(offset + length, AlignUp4K(handle.size));

  const Status check = Check(kWantExec);
  if (!check.ok()) {
    co_return check;
  }

  const std::optional<CacheManager::Entry> entry = manager_->Touch(handle);
  if (!entry && !loader_->IsLoading()) {
    misses_++;
    co_return Status::NotFound("block not cached");
  }

  const bool staged = entry && entry->staged;
  const std::string path =
      staged ? GetStagePath(entry->handle) : GetCachePath(handle);

  Status status = co_await localfs_->ReadFile(path, offset, length, buffer);
  if (status.IsNotExist()) {
    if (!staged) {
      manager_->EraseCache(handle);
    }
    status = Status::NotFound("block not cached");
  }

  if (status.ok()) {
    hits_++;
  } else {
    misses_++;
  }
  co_return status;
}

Future<Status> DiskCache::Delete(BlockHandle handle) {
  const Status check = Check(kWantExec);
  if (!check.ok()) {
    co_return check;
  }

  const std::optional<CacheManager::Entry> entry = manager_->Find(handle);
  if (!entry) {
    if (loader_->IsLoading()) {
      const Status status = co_await localfs_->Unlink(GetCachePath(handle));
      co_return status.IsNotExist() ? Status::OK() : status;
    }
    co_return Status::OK();
  } else if (entry->staged) {
    co_return Status::OK();
  }

  manager_->EraseCache(handle);
  const Status status = co_await localfs_->Unlink(GetCachePath(handle));
  co_return status.IsNotExist() ? Status::OK() : status;
}

Future<bool> DiskCache::Exists(BlockHandle handle) {
  if (manager_->Exist(handle)) {
    co_return true;
  } else if (loader_->IsLoading()) {
    co_return co_await localfs_->FileExists(GetCachePath(handle));
  }
  co_return false;
}

Future<CacheStats> DiskCache::GetStats() {
  CacheStats stats = manager_->GetStats();
  stats.hits += hits_;
  stats.misses += misses_;
  stats.io_errors += health_->io_errors();
  stats.disks.push_back(DiskStats{.uuid = uuid_,
                                  .dir = layout_.RootDir(),
                                  .capacity_bytes = stats.capacity_bytes,
                                  .used_bytes = stats.used_bytes,
                                  .health = health_->state(),
                                  .stage_full = manager_->StageFull(),
                                  .cache_full = manager_->CacheFull()});
  return MakeReadyFuture<CacheStats>(std::move(stats));
}

Status DiskCache::CreateDirs() {
  std::error_code ec;
  for (const std::string& dir :
       {layout_.StageDir(), layout_.CacheDir(), layout_.ProbeDir()}) {
    std::filesystem::create_directories(dir, ec);
    if (ec) {
      return Status::IoError("create " + dir + ": " + ec.message());
    }
  }
  return Status::OK();
}

Status DiskCache::GetOrCreateLockFile() {
  const std::string path = layout_.LockPath();

  // read uuid from file
  {
    std::ifstream in(path);
    std::getline(in, uuid_);
    if (!uuid_.empty()) {
      return Status::OK();
    }
  }

  // write uuid to file
  {
    const std::string tmp = path + ".i" + std::to_string(ThisShardId());
    std::ofstream out(tmp, std::ios::trunc);
    out << utils::GenerateUUID();
    out.close();
    if (!out.good()) {
      return Status::IoError("write " + tmp);
    }

    ::link(tmp.c_str(), path.c_str());
    ::unlink(tmp.c_str());

    std::ifstream in(path);
    std::getline(in, uuid_);
  }

  return uuid_.empty() ? Status::IoError("read uuid failed") : Status::OK();
}

Status DiskCache::Check(uint8_t want) const {
  if (!health_->IsNormal()) {
    return Status::CacheDown("disk cache is down");
  } else if ((want & kWantStage) && manager_->StageFull()) {
    return Status::CacheFull("disk stage full");
  } else if ((want & kWantCache) && manager_->CacheFull()) {
    return Status::CacheFull("disk cache full");
  }
  return Status::OK();
}

}  // namespace blockcache
}  // namespace dingofs
