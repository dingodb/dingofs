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
#include <butil/time.h>

#include <atomic>

#include "cache/blockcache/cache_store.h"
#include "cache/common/macro.h"
#include "cache/storage/base_filesystem.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

DiskCacheLoader::DiskCacheLoader(DiskCacheLayoutSPtr layout,
                                 std::shared_ptr<DiskCacheManager> manager,
                                 DiskCacheMetricSPtr metric)
    : running_(false),
      cache_loading_(false),
      stage_loading_(false),
      layout_(layout),
      manager_(manager),
      thread_pool_(std::make_unique<TaskThreadPool>("disk_cache_loader")),
      metric_(metric) {}

void DiskCacheLoader::Start(const std::string& disk_id,
                            CacheStore::UploadFunc uploader) {
  CHECK_NOTNULL(layout_);
  CHECK_NOTNULL(manager_);
  CHECK_NOTNULL(thread_pool_);
  CHECK_NOTNULL(metric_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Disk cache loader is starting...";

  disk_id_ = disk_id;
  uploader_ = uploader;
  cache_loading_ = true;
  stage_loading_ = true;
  metric_->load_status.set_value("loading");

  CHECK_EQ(thread_pool_->Start(2), 0);
  thread_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                        layout_->GetStageDir(), BlockType::kStageBlock);
  thread_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                        layout_->GetCacheDir(), BlockType::kCacheBlock);

  running_ = true;

  LOG(INFO) << "Disk cache loader is up.";

  CHECK_RUNNING("Disk cache loader");
}

void DiskCacheLoader::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Disk cache loader is shutting down...";

  thread_pool_->Stop();

  metric_->load_status.set_value("stoped");

  LOG(INFO) << "Disk cache loader is down.";

  CHECK_DOWN("Disk cache loader");
}

// If load failed, it only takes up some spaces.
void DiskCacheLoader::LoadAllBlocks(const std::string& dir, BlockType type) {
  butil::Timer timer;
  Status status;
  uint64_t num_blocks = 0, num_invalids = 0, size = 0;

  timer.start();
  status =
      FSUtil::Walk(dir, [&](const std::string& prefix, const FileInfo& file) {
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

  std::string message = absl::StrFormat(
      "Load %s (dir=%s) %s: %d blocks loaded, %d invalid blocks found, costs "
      "%.6lf seconds.",
      ToString(type), dir, status.ToString(), num_blocks, num_invalids,
      timer.u_elapsed() / 1e6);

  LOG(INFO) << message;
  if (!status.ok()) {
    LOG(ERROR) << "Load blocks from disk failed: " << status.ToString();
  }

  if (type == BlockType::kCacheBlock) {
    cache_loading_.store(false, std::memory_order_relaxed);
  } else if (type == BlockType::kStageBlock) {
    stage_loading_.store(false, std::memory_order_relaxed);
  } else {
    CHECK(false) << "Unknown block type: " << static_cast<int>(type);
  }

  if (!cache_loading_ && !stage_loading_) {
    metric_->load_status.set_value("finish");
  }
}

bool DiskCacheLoader::LoadOneBlock(const std::string& prefix,
                                   const FileInfo& file, BlockType type) {
  BlockKey key;
  std::string name = file.name;
  std::string path = Helper::PathJoin({prefix, name});

  if (Helper::IsTempFilepath(name) || !key.ParseFromFilename(name)) {
    auto status = FSUtil::RemoveFile(path);
    if (status.ok()) {
      LOG(INFO) << "Remove invalid block (path=" << path << ") success.";
    } else {
      LOG(WARNING) << "Remove invalid block (path=" << path
                   << ") failed: " << status.ToString();
    }
    return false;
  }

  if (type == BlockType::kStageBlock) {
    manager_->Add(key, CacheValue(file.size, file.atime), BlockPhase::kStaging);
    uploader_(NewContext(), key, file.size,
              BlockContext(BlockContext::kFromReload, disk_id_));
  } else if (type == BlockType::kCacheBlock) {
    if (file.nlink == 1) {
      manager_->Add(key, CacheValue(file.size, file.atime),
                    BlockPhase::kCached);
    }
  } else {
    LOG(FATAL) << "Unknown block type: block type = " << static_cast<int>(type)
               << ", path = " << path << ", file size = " << file.size
               << ", file nlink = " << file.nlink;
  }

  return true;
}

bool DiskCacheLoader::IsLoading() {
  return cache_loading_.load(std::memory_order_relaxed);
}

std::string DiskCacheLoader::GetStageDir() const {
  return layout_->GetStageDir();
}

std::string DiskCacheLoader::GetCacheDir() const {
  return layout_->GetCacheDir();
}

std::string DiskCacheLoader::ToString(BlockType type) const {
  switch (type) {
    case BlockType::kStageBlock:
      return "stage";
    case BlockType::kCacheBlock:
      return "cache";
    default:
      CHECK(false) << "Unknown block type: " << static_cast<int>(type);
  }
}

}  // namespace cache
}  // namespace dingofs
