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

#include "curvefs/src/client/blockcache/disk_cache_loader.h"

#include <butil/time.h>

#include <atomic>

#include "curvefs/src/base/filepath/filepath.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/disk_cache_metric.h"
#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::butil::Timer;
using ::curvefs::base::filepath::HasSuffix;
using ::curvefs::base::filepath::PathJoin;

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

void DiskCacheLoader::Start(CacheStore::UploadFunc uploader) {
  if (running_.exchange(true)) {
    return;  // already running
  }

  uploader_ = uploader;
  task_pool_->Start(2);
  task_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                      layout_->GetStageDir(), BlockType::STAGE_BLOCK);
  task_pool_->Enqueue(&DiskCacheLoader::LoadAllBlocks, this,
                      layout_->GetCacheDir(), BlockType::CACHE_BLOCK);

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
  BCACHE_ERROR rc;
  uint64_t num_blocks = 0, num_invalids = 0, size = 0;

  timer.start();
  rc = fs_->Walk(root, [&](const std::string& prefix, const FileInfo& file) {
    if (!running_.load(std::memory_order_relaxed)) {
      return BCACHE_ERROR::ABORT;
    }

    if (LoadOneBlock(prefix, file, type)) {
      num_blocks++;
      size += file.size;
    } else {
      num_invalids++;
    }
    return BCACHE_ERROR::OK;
  });
  timer.stop();

  std::string message = StrFormat(
      "Load %s (dir=%s) %s: %d blocks loaded, %d invalid blocks found, costs "
      "%.6f seconds.",
      ToString(type), root, StrErr(rc), num_blocks, num_invalids,
      timer.u_elapsed() / 1e6);
  if (rc == BCACHE_ERROR::OK) {
    LOG(INFO) << message;
  } else {
    LOG(ERROR) << message;
  }

  if (type == BlockType::CACHE_BLOCK) {
    metric_->SetLoadStatus(kLoadFinised);
  }
}

bool DiskCacheLoader::LoadOneBlock(const std::string& prefix,
                                   const FileInfo& file, BlockType type) {
  BlockKey key;
  std::string name = file.name;
  std::string path = PathJoin({prefix, name});

  if (HasSuffix(name, ".tmp") || !key.ParseFilename(name)) {
    LOG(INFO) << "Remove invalid block, filename=" << name;
    auto rc = fs_->RemoveFile(path);
    if (rc != BCACHE_ERROR::OK) {
      LOG(WARNING) << "Remove invalid block failed: " << StrErr(rc);
    }
    return false;
  }

  if (type == BlockType::STAGE_BLOCK) {
    metric_->AddStageBlock(1);
    uploader_(key, path, BlockContext(BlockFrom::RELOAD));
  } else if (type == BlockType::CACHE_BLOCK) {
    manager_->Add(key, CacheValue(file.size, file.atime));
  }
  return true;
}

std::string DiskCacheLoader::ToString(BlockType type) {
  if (type == BlockType::STAGE_BLOCK) {
    return "stage";
  } else if (type == BlockType::CACHE_BLOCK) {
    return "cache";
  }
  return "unknown";
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
