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

#include "cache/local/disk_cache_loader.h"

#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <butil/time.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <thread>
#include <utility>

#include "cache/local/cache_store.h"
#include "cache/common/block_handle_helper.h"
#include "cache/iutil/file_util.h"

namespace dingofs {
namespace cache {

namespace fs = std::filesystem;

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
  if (running_.exchange(true, std::memory_order_relaxed)) {
    LOG(WARNING) << "DiskCacheLoader already started";
    return;
  }

  LOG(INFO) << "DiskCacheLoader is starting...";

  disk_id_ = disk_id;
  uploader_ = std::move(uploader);
  still_loading_stage_.store(true, std::memory_order_relaxed);
  still_loading_cache_.store(true, std::memory_order_relaxed);
  load_status_.set_value("loading");

  t1_ = std::thread([this]() { LoadAllBlocks(BlockType::kStageBlock); });
  t2_ = std::thread([this]() { LoadAllBlocks(BlockType::kCacheBlock); });

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

void DiskCacheLoader::LoadAllBlocks(BlockType type) {
  butil::Timer timer;
  timer.start();
  LoadStats stats;

  if (type == BlockType::kStageBlock) {
    LoadStageDir(&stats);
  } else {
    LoadCacheDir(&stats);
  }

  timer.stop();
  LOG(INFO) << "Loaded " << stats.num_blocks << " " << BlockTypeToString(type)
            << "(s), removed " << stats.num_invalids
            << " invalid file(s), took " << timer.u_elapsed() / 1e6 << "s";

  auto& flag = (type == BlockType::kCacheBlock) ? still_loading_cache_
                                                : still_loading_stage_;
  flag.store(false, std::memory_order_relaxed);
  if (!still_loading_cache_.load(std::memory_order_relaxed) &&
      !still_loading_stage_.load(std::memory_order_relaxed)) {
    load_status_.set_value("finish");
  }
}

void DiskCacheLoader::LoadStageDir(LoadStats* stats) {
  // stage/{fs_id}/{kStoreDir}/... — subdir names come from the key types
  // themselves (single source of truth, see {Block,Tensor}Key::kStoreDir).
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator(layout_->GetStageDir(), ec)) {
    if (!running_.load(std::memory_order_relaxed)) {
      return;
    }
    if (!entry.is_directory()) {
      continue;
    }

    uint32_t fs_id = static_cast<uint32_t>(
        std::strtoul(entry.path().filename().c_str(), nullptr, 10));

    LoadKindDir<BlockKey>(
        (entry.path() / BlockKey::kStoreDir).string(), BlockType::kStageBlock,
        [fs_id](BlockKey k) { return BlockHandle(fs_id, k); }, stats);
    LoadKindDir<TensorKey>(
        (entry.path() / TensorKey::kStoreDir).string(),
        BlockType::kStageBlock,
        [](TensorKey k) { return BlockHandle(std::move(k)); }, stats);
  }
}

void DiskCacheLoader::LoadCacheDir(LoadStats* stats) {
  // cache/{kStoreDir}/...
  fs::path cache_dir(layout_->GetCacheDir());
  LoadKindDir<BlockKey>(
      (cache_dir / BlockKey::kStoreDir).string(), BlockType::kCacheBlock,
      [](BlockKey k) { return BlockHandle(0, k); }, stats);
  LoadKindDir<TensorKey>(
      (cache_dir / TensorKey::kStoreDir).string(), BlockType::kCacheBlock,
      [](TensorKey k) { return BlockHandle(std::move(k)); }, stats);
}

template <typename KeyT, typename Wrap>
void DiskCacheLoader::LoadKindDir(const std::string& root, BlockType type,
                                  Wrap&& wrap, LoadStats* stats) {
  std::error_code ec;
  if (!fs::exists(root, ec)) {
    return;  // empty / never-written kind
  }

  iutil::Walk(root,
              [&](const std::string& prefix, const iutil::FileInfo& file) {
                if (!running_.load(std::memory_order_relaxed)) {
                  return Status::Abort("disk cache loader stopped");
                }

                std::string path = absl::StrJoin({prefix, file.name}, "/");
                if (IsTempFilepath(file.name)) {
                  RemoveInvalidBlock(path, "temp");
                  stats->num_invalids++;
                  return Status::OK();
                }

                KeyT key;
                if (!ParseFromFilename(file.name, &key)) {
                  RemoveInvalidBlock(path, "invalid");
                  stats->num_invalids++;
                  return Status::OK();
                }

                RegisterBlock(wrap(std::move(key)), file, type);
                stats->num_blocks++;
                stats->total_bytes += file.size;
                return Status::OK();
              });
}

void DiskCacheLoader::RegisterBlock(const BlockHandle& handle,
                                    const iutil::FileInfo& file,
                                    BlockType type) {
  switch (type) {
    case BlockType::kStageBlock:
      manager_->Add(handle, CacheValue(file.size, file.atime),
                    BlockPhase::kStaging);
      uploader_(handle, file.size, BlockAttr(BlockAttr::kFromReload, disk_id_));
      break;
    case BlockType::kCacheBlock:
      if (file.nlink == 1) {
        manager_->Add(handle, CacheValue(file.size, file.atime),
                      BlockPhase::kCached);
      }
      break;
  }
}

void DiskCacheLoader::RemoveInvalidBlock(const std::string& path,
                                         const char* reason) {
  auto status = iutil::Unlink(path);
  if (status.ok()) {
    LOG(INFO) << "Removed " << reason << " block, path=`" << path << "'";
  } else {
    LOG(WARNING) << "Fail to remove " << reason << " block, path=`" << path
                 << "'";
  }
}

}  // namespace cache
}  // namespace dingofs
