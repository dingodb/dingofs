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

#include <butil/time.h>

#include <atomic>

#include "base/filepath/filepath.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_metric.h"
#include "cache/common/common.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using base::filepath::HasSuffix;
using base::filepath::PathJoin;
using butil::Timer;

DiskCacheLoader::DiskCacheLoader(std::shared_ptr<DiskCacheLayout> layout,
                                 std::shared_ptr<LocalFileSystem> fs,
                                 std::shared_ptr<DiskCacheManager> manager,
                                 std::shared_ptr<DiskCacheMetric> metric)
    : running_(false),
      layout_(layout),
      fs_(fs),
      manager_(manager),
      metric_(metric),
      task_pool_(absl::make_unique<TaskThreadPool<>>("disk_cache_loader")) {}

void DiskCacheLoader::Start(const std::string& disk_id,
                            CacheStore::UploadFunc uploader) {
  if (running_.exchange(true)) {
    return;  // already running
  }

  disk_id_ = disk_id;
  uploader_ = uploader;
  task_pool_->Start(2);
  task_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                      layout_->GetStageDir(), BlockType::kStageBlock);
  task_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                      layout_->GetCacheDir(), BlockType::kCacheBlock);

  metric_->SetLoadStatus(kOnLoading);
  LOG(INFO) << "Disk cache loading thread start success.";
}

void DiskCacheLoader::Stop() {
  if (!running_.exchange(false)) {
    return;  // already stopped
  }

  LOG(INFO) << "Stop disk cache loading thread...";
  task_pool_->Stop();
  metric_->SetLoadStatus(kLoadStopped);
  LOG(INFO) << "Disk cache loading thread stopped.";
}

bool DiskCacheLoader::IsLoading() {
  return metric_->GetLoadStatus() != kLoadFinised;
}

// If load failed, it only takes up some spaces.
void DiskCacheLoader::LoadAllBlocks(const std::string& root, BlockType type) {
  Timer timer;
  Status status;
  uint64_t num_blocks = 0, num_invalids = 0, size = 0;

  timer.start();
  status =
      fs_->Walk(root, [&](const std::string& prefix, const FileInfo& file) {
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

  std::string message = StrFormat(
      "Load %s (dir=%s) %s: %d blocks loaded, %d invalid blocks found, costs "
      "%.6f seconds.",
      StrType(type), root, status.ToString(), num_blocks, num_invalids,
      timer.u_elapsed() / 1e6);
  if (status.ok()) {
    LOG(INFO) << message;
  } else {
    LOG(ERROR) << message;
  }

  if (type == BlockType::kCacheBlock) {
    metric_->SetLoadStatus(kLoadFinised);
  }
}

bool DiskCacheLoader::LoadOneBlock(const std::string& prefix,
                                   const FileInfo& file, BlockType type) {
  BlockKey key;
  std::string name = file.name;
  std::string path = PathJoin({prefix, name});

  if (HasSuffix(name, ".tmp") || !key.ParseFilename(name)) {
    auto status = fs_->RemoveFile(path);
    if (status.ok()) {
      LOG(INFO) << "Remove invalid block (path=" << path << ") success.";
    } else {
      LOG(WARNING) << "Remove invalid block (path=" << path
                   << ") failed: " << status.ToString();
    }
    return false;
  }

  if (type == BlockType::kStageBlock) {
    metric_->AddStageBlock(1);
    uploader_(key, path, BlockContext(BlockFrom::kReload, disk_id_));
  } else if (type == BlockType::kCacheBlock) {
    manager_->Add(key, CacheValue(file.size, file.atime));
  }
  return true;
}

std::string DiskCacheLoader::StrType(BlockType type) {
  if (type == BlockType::kStageBlock) {
    return "stage";
  } else if (type == BlockType::kCacheBlock) {
    return "cache";
  }
  return "unknown";
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
