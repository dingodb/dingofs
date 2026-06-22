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

#ifndef DINGOFS_SRC_CACHE_LOCAL_DISK_CACHE_LOADER_H_
#define DINGOFS_SRC_CACHE_LOCAL_DISK_CACHE_LOADER_H_

#include <atomic>
#include <thread>

#include "cache/iutil/file_util.h"
#include "cache/local/cache_store.h"
#include "cache/local/disk_cache_layout.h"
#include "cache/local/disk_cache_manager.h"
#include "common/block/block_handle.h"

namespace dingofs {
namespace cache {

// On-disk layout (see disk_cache_layout.h):
//   {root}/stage/{fs_id}/{blocks|tensor}/.../{filename}
//   {root}/cache/{blocks|tensor}/.../{filename}
//
// The loader mirrors this layout in its traversal so that for any file it
// already knows (a) the kind — block or tensor — and (b) the fs_id, without
// re-deriving them from the path on every file.
class DiskCacheLoader {
 public:
  DiskCacheLoader(DiskCacheLayoutSPtr layout, DiskCacheManagerSPtr manager);
  void Start(const std::string& disk_id, CacheStore::UploadFunc uploader);
  void Shutdown();

  bool StillLoading() {
    return still_loading_stage_.load(std::memory_order_relaxed) ||
           still_loading_cache_.load(std::memory_order_relaxed);
  }

 private:
  enum class BlockType : uint8_t {
    kStageBlock = 0,
    kCacheBlock = 1,
  };

  std::string BlockTypeToString(BlockType type) const {
    switch (type) {
      case BlockType::kStageBlock:
        return "stage block";
      case BlockType::kCacheBlock:
        return "cache block";
      default:
        CHECK(false) << "unknown block type=" << static_cast<uint8_t>(type);
    }
  }

  struct LoadStats {
    uint64_t num_blocks{0};
    uint64_t num_invalids{0};
    uint64_t total_bytes{0};
  };

  // Top-level entry: dispatches to LoadStageDir / LoadCacheDir.
  void LoadAllBlocks(BlockType type);

  // Layout-aware walkers: enumerate {fs_id} / {blocks,tensor} subdirs and
  // delegate to LoadKindDir for the per-key-type leaf walk.
  void LoadStageDir(LoadStats* stats);
  void LoadCacheDir(LoadStats* stats);

  // Walks a single {blocks} or {tensor} subtree. KeyT picks the parser
  // (BlockKey or TensorKey) and Wrap lifts the parsed key into a BlockHandle
  // — fs_id is closed over in the wrapper, not re-parsed per file.
  template <typename KeyT, typename Wrap>
  void LoadKindDir(const std::string& root, BlockType type, Wrap&& wrap,
                   LoadStats* stats);

  // Adds a successfully-parsed handle to the manager and, for stage blocks,
  // triggers the upload callback.
  void RegisterBlock(const BlockHandle& handle, const iutil::FileInfo& file,
                     BlockType type);

  static void RemoveInvalidBlock(const std::string& path, const char* reason);

  std::atomic<bool> running_;
  DiskCacheLayoutSPtr layout_;
  DiskCacheManagerSPtr manager_;
  std::string disk_id_;
  CacheStore::UploadFunc uploader_;
  std::atomic<bool> still_loading_cache_{true};
  std::atomic<bool> still_loading_stage_{true};
  std::thread t1_, t2_;
  bvar::Status<std::string> load_status_;
};

using DiskCacheLoaderUPtr = std::unique_ptr<DiskCacheLoader>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_DISK_CACHE_LOADER_H_
