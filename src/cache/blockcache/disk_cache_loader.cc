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

#include "cache/blockcache/disk_cache_loader.h"

#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <butil/time.h>

#include <atomic>
#include <thread>

#include "cache/blockcache/cache_store.h"
#include "cache/common/macro.h"
#include "cache/iutil/file_util.h"

namespace dingofs {
namespace cache {

DiskCacheLoader::DiskCacheLoader(DiskCacheLayoutSPtr layout,
                                 std::shared_ptr<DiskCacheManager> manager)
    : running_(false),
      layout_(layout),
      manager_(manager),
      load_status_(absl::StrFormat("dingofs_disk_cache_%d_load_status",
                                   layout->CacheIndex()),
                   "down") {}

void DiskCacheLoader::Start(const std::string& disk_id,
                            CacheStore::UploadFunc uploader) {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "DiskCacheLoader already started";
    return;
  }

  LOG(INFO) << "DiskCacheLoader is starting...";

  disk_id_ = disk_id;
  uploader_ = uploader;

  t1_ = std::thread([this]() {
    LoadAllBlocks(layout_->GetStageDir(), BlockType::kStageBlock);
  });
  t2_ = std::thread([this]() {
    LoadAllBlocks(layout_->GetCacheDir(), BlockType::kCacheBlock);
  });

  running_.store(true, std::memory_order_relaxed);
  load_status_.set_value("loading");
  LOG(INFO) << "DiskCacheLoader started";
}

void DiskCacheLoader::Shutdown() {
  if (!running_.exchange(false)) {
    LOG(WARNING) << "DiskCacheLoader already down";
    return;
  }

  LOG(INFO) << "DiskCacheLoader is shutting down...";

  t1_.join();
  t2_.join();
  load_status_.set_value("stopped");

  LOG(INFO) << "DiskCacheLoader is down";
}

// If load failed, it only takes up some spaces.
void DiskCacheLoader::LoadAllBlocks(const std::string& dir, BlockType type) {
  butil::Timer timer;
  uint64_t num_blocks = 0, num_invalids = 0, size = 0;

  timer.start();
  auto status = iutil::Walk(
      dir, [&](const std::string& prefix, const iutil::FileInfo& file) {
        if (!running_.load(std::memory_order_relaxed)) {
          return Status::Abort("disk cache loader stopped");
        }

        if (LoadOneBlock(prefix, file, type)) {
          num_blocks++;
          size += file.size;
        } else {
          num_invalids++;
        }
        return Status::OK();
      });
  timer.stop();

  if (status.ok()) {
    LOG(INFO) << "Successfully load " << num_blocks << BlockTypeToString(type)
              << " blocks form dir=`" << dir << "' " << num_invalids
              << " invalid blocks found, tooks " << timer.u_elapsed() / 1e6
              << " seconds";
  } else {
    LOG(ERROR) << "Fail to load " << BlockTypeToString(type)
               << " block from dir=`" << dir
               << "', status=" << status.ToString();
  }

  if (type == BlockType::kCacheBlock) {
    still_loading_cache_.store(false, std::memory_order_relaxed);
  } else if (type == BlockType::kStageBlock) {
    still_loading_stage_.store(false, std::memory_order_relaxed);
  } else {
    CHECK(false) << "Unknown block type=" << static_cast<int>(type);
  }

  if (!still_loading_cache_.load(std::memory_order_relaxed) &&
      !still_loading_stage_.load(std::memory_order_relaxed)) {
    load_status_.set_value("finish");
  }
}

bool DiskCacheLoader::LoadOneBlock(const std::string& prefix,
                                   const iutil::FileInfo& file,
                                   BlockType type) {
  BlockKey key;
  std::string name = file.name;
  std::string path = absl::StrJoin({prefix, name}, "/");
  if (IsTempFilepath(name) || !key.ParseFromFilename(name)) {
    auto status = iutil::Unlink(path);
    if (status.ok()) {
      LOG(INFO) << "Removed invalid block, path=`" << path << "'";
    } else {
      LOG(WARNING) << "Fail to remove invalid block, path=`" << path << "'";
    }
    return false;
  }

  if (type == BlockType::kStageBlock) {
    manager_->Add(key, CacheValue(file.size, file.atime), BlockPhase::kStaging);
    uploader_(NewContext(), key, file.size,
              BlockAttr(BlockAttr::kFromReload, disk_id_));
  } else if (type == BlockType::kCacheBlock) {
    if (file.nlink == 1) {
      manager_->Add(key, CacheValue(file.size, file.atime),
                    BlockPhase::kCached);
    }
  } else {
    LOG(FATAL) << "Unknown block type=" << static_cast<int>(type)
               << ", filepath=`" << path << "', filesize=" << file.size
               << ", nlink=" << file.nlink;
  }

  return true;
}

}  // namespace cache
}  // namespace dingofs
