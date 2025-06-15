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

#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

using base::filepath::HasSuffix;

DiskCacheLoader::DiskCacheLoader(DiskCacheLayoutSPtr layout,
                                 std::shared_ptr<DiskCacheManager> manager)
    : running_(false),
      loading_(false),
      layout_(layout),
      manager_(manager),
      task_pool_(std::make_unique<TaskThreadPool>("disk_cache_loader")) {}

void DiskCacheLoader::Start(const std::string& disk_id,
                            CacheStore::UploadFunc uploader) {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Disk cache loader starting...";

    disk_id_ = disk_id;
    uploader_ = uploader;
    loading_.store(true, std::memory_order_acq_rel);

    CHECK_EQ(task_pool_->Start(2), 0);
    task_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                        layout_->GetStageDir(), BlockType::kStageBlock);
    task_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                        layout_->GetCacheDir(), BlockType::kCacheBlock);

    LOG(INFO) << "Disk cache loader started.";
  }
}

void DiskCacheLoader::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Disk cache loader stopping...";

    task_pool_->Stop();
    loading_.store(false, std::memory_order_acq_rel);

    LOG(INFO) << "Disk cache loader stopped.";
  }
}

// If load failed, it only takes up some spaces.
void DiskCacheLoader::LoadAllBlocks(const std::string& root, BlockType type) {
  butil::Timer timer;
  Status status;
  uint64_t num_blocks = 0, num_invalids = 0, size = 0;

  timer.start();
  status =
      Helper::Walk(root, [&](const std::string& prefix, const FileInfo& file) {
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
      "%.6f seconds.",
      ToString(type), root, status.ToString(), num_blocks, num_invalids,
      timer.u_elapsed() / 1e6);

  LOG(INFO) << message;
  if (!status.ok()) {
    LOG(ERROR) << "Load blocks from disk failed: " << status.ToString();
  }

  if (type == BlockType::kCacheBlock) {
    loading_.store(false, std::memory_order_acq_rel);
  }
}

bool DiskCacheLoader::LoadOneBlock(const std::string& prefix,
                                   const FileInfo& file, BlockType type) {
  BlockKey key;
  std::string name = file.name;
  std::string path = base::filepath::PathJoin({prefix, name});

  if (Helper::IsTempFilepath(name) || !key.ParseFilename(name)) {
    auto status = Helper::RemoveFile(path);
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
    uploader_(key, file.size, BlockContext(BlockFrom::kReload, disk_id_));
  } else if (type == BlockType::kCacheBlock && file.nlink == 1) {
    manager_->Add(key, CacheValue(file.size, file.atime), BlockPhase::kCached);
  }
  return true;
}

bool DiskCacheLoader::IsLoading() {
  return loading_.load(std::memory_order_acquire);
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
      return "unknown";
  }
}

}  // namespace cache
}  // namespace dingofs
