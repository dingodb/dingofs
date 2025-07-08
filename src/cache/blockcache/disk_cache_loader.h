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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_LOADER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_LOADER_H_

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "cache/storage/filesystem.h"

namespace dingofs {
namespace cache {

class DiskCacheLoader {
 public:
  DiskCacheLoader(DiskCacheLayoutSPtr layout, DiskCacheManagerSPtr manager,
                  DiskCacheMetricSPtr metric);
  virtual ~DiskCacheLoader() = default;

  virtual void Start(const std::string& disk_id,
                     CacheStore::UploadFunc uploader);
  virtual void Shutdown();

  virtual bool IsLoading();

 private:
  enum class BlockType : uint8_t {
    kStageBlock = 0,
    kCacheBlock = 1,
  };

  void LoadAllBlocks(const std::string& dir, BlockType type);
  bool LoadOneBlock(const std::string& prefix, const FileInfo& file,
                    BlockType type);

  std::string GetStageDir() const;
  std::string GetCacheDir() const;
  std::string ToString(BlockType type) const;

  std::atomic<bool> running_;
  std::atomic<bool> cache_loading_;
  std::atomic<bool> stage_loading_;
  std::string disk_id_;
  CacheStore::UploadFunc uploader_;
  DiskCacheLayoutSPtr layout_;
  DiskCacheManagerSPtr manager_;
  TaskThreadPoolUPtr thread_pool_;
  DiskCacheMetricSPtr metric_;
};

using DiskCacheLoaderUPtr = std::unique_ptr<DiskCacheLoader>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_LOADER_H_
