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

#include "blockcache/store/disk_cache_loader.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "blockcache/common/route.h"
#include "blockcache/core/fs/filesystem.h"
#include "blockcache/core/reactor/preempt.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/store/local_filesystem.h"
#include "blockcache/utils/string.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

DiskCacheLoader::DiskCacheLoader(const DiskCacheLayout& layout,
                                 CacheManager* manager)
    : layout_(layout), manager_(manager) {}

Future<> DiskCacheLoader::Start(UploadFunc uploader) {
  LOG(INFO) << "DiskCacheLoader is starting...";

  running_ = true;
  loading_ = true;
  uploader_ = std::move(uploader);
  Future<> worker = LoadAllBlocks();

  LOG(INFO) << "Successfully start DiskCacheLoader";
  return worker;
}

Future<> DiskCacheLoader::Shutdown() {
  LOG(INFO) << "DiskCacheLoader is shutting down...";

  running_ = false;
  co_await gate_.Close();

  LOG(INFO) << "Successfully shutdown DiskCacheLoader";
}

Future<> DiskCacheLoader::LoadAllBlocks() {
  Gate::Holder holder(gate_);
  CHECK(holder.ok()) << "DiskCacheLoader is down";
  co_await Yield();

  const uint64_t begin = TimestampNs();
  try {
    co_await WhenAll(LoadStageBlocks(), LoadCacheBlocks());
    LOG(INFO) << "Successfully load all blocks, cost "
              << fmt::format("{:.3f}", (TimestampNs() - begin) / 1e9)
              << " seconds";
  } catch (const std::exception& e) {
    LOG(ERROR) << "Fail to load all blocks: " << e.what();
  }
  loading_ = false;
}

Future<> DiskCacheLoader::LoadStageBlocks() {
  std::vector<std::pair<std::string, uint64_t>> dirs;
  std::error_code ec;
  std::filesystem::directory_iterator it(layout_.StageDir(), ec);
  for (; !ec && it != std::filesystem::directory_iterator(); it.increment(ec)) {
    uint64_t fs_id = 0;
    std::string_view name = Filename(it->path().native());
    if (SplitUint(&name, '\0', &fs_id)) {
      dirs.emplace_back(it->path().string(), fs_id);
    }
  }

  LOG_IF(WARNING, ec) << "Fail to walk stage dir=`" << layout_.StageDir()
                      << "': " << ec.message();

  Result total;
  for (auto& [dir, fs_id] : dirs) {
    total += co_await LoadDir(std::move(dir), fs_id, true);
  }

  LOG(INFO) << "Successfully load stage dir=`" << layout_.StageDir()
            << "': " << total.loaded << " blocks loaded, " << total.removed
            << " invalid blocks removed, cost "
            << fmt::format("{:.3f}", total.elapsed_ns / 1e9) << " seconds";
}

Future<> DiskCacheLoader::LoadCacheBlocks() {
  const Result result = co_await LoadDir(layout_.CacheDir(), 0, false);

  LOG(INFO) << "Successfully load cache dir=`" << layout_.CacheDir()
            << "': " << result.loaded << " blocks loaded, " << result.removed
            << " invalid blocks removed, cost "
            << fmt::format("{:.3f}", result.elapsed_ns / 1e9) << " seconds";
}

Future<DiskCacheLoader::Result> DiskCacheLoader::LoadDir(std::string dir,
                                                         uint64_t fs_id,
                                                         bool staged) {
  Result result;
  const uint64_t began = TimestampNs();

  std::vector<Future<>> inflight;
  inflight.reserve(kBatchSize);

  std::error_code ec;
  std::filesystem::recursive_directory_iterator it(dir, ec);
  for (; !ec && it != std::filesystem::recursive_directory_iterator();
       it.increment(ec)) {
    if (!running_) {
      break;
    }

    if (NeedPreempt()) {
      co_await Yield();
    }

    std::error_code type_ec;
    if (!it->is_regular_file(type_ec)) {
      continue;
    }
    const std::string& path = it->path().native();
    const std::string_view name = Filename(path);

    BlockHandle handle{.fs_id = fs_id};
    if (DiskCacheLayout::IsTempFilename(name) ||
        !handle.ParseFromFilename(name)) {
      inflight.push_back(Unlink(path, &result));
    } else if (OwnerShard(handle) == ThisShardId()) {
      inflight.push_back(LoadBlock(path, handle, staged, &result));
    }

    if (inflight.size() >= kBatchSize) {
      co_await WhenAll(std::exchange(inflight, {}));
    }
  }
  LOG_IF(WARNING, ec) << "Fail to walk dir=`" << dir << "': " << ec.message();

  co_await WhenAll(std::move(inflight));

  result.elapsed_ns = TimestampNs() - began;
  co_return result;
}

Future<> DiskCacheLoader::LoadBlock(std::string path, BlockHandle handle,
                                    bool staged, Result* result) {
  StatusOr<FileStat> stat = co_await FileSystem::StatPath(path);
  if (!stat.ok()) {
    LOG(ERROR) << "Fail to stat block file, path=`" << path
               << "': " << stat.status().ToString();
    co_return;
  } else if (!staged && stat->nlink > 1) {  // cached block and linked to stage
    co_return;
  } else if (manager_->Find(handle)) {
    LOG(WARNING) << "Skip already inserted block, path=`" << path << "'";
    co_return;
  }

  manager_->Insert(handle, SteadyAtime(stat->atime_sec), staged);
  result->loaded++;
  if (staged) {
    uploader_(handle);
  }
}

Future<> DiskCacheLoader::Unlink(std::string path, Result* result) {
  const Status status = co_await FileSystem::Unlink(path);
  if (status.ok()) {
    result->removed++;
    LOG(INFO) << "Successfully remove invalid block, path=`" << path << "'";
  } else if (!status.IsNotExist()) {
    LOG(WARNING) << "Fail to remove invalid block, path=`" << path
                 << "': " << status.ToString();
  }
}

std::string_view DiskCacheLoader::Filename(const std::string& path) {
  return std::string_view(path).substr(path.rfind('/') + 1);
}

uint32_t DiskCacheLoader::SteadyAtime(int64_t atime_sec) {
  const int64_t age =
      std::max<int64_t>(0, static_cast<int64_t>(::time(nullptr)) - atime_sec);
  const uint32_t now = TimestampSec();
  return now > age ? now - static_cast<uint32_t>(age) : 0;
}

}  // namespace blockcache
}  // namespace dingofs
