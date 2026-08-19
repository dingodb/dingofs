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

#ifndef DINGOFS_BLOCKCACHE_STORE_DISK_CACHE_LOADER_H_
#define DINGOFS_BLOCKCACHE_STORE_DISK_CACHE_LOADER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/store/cache_manager.h"
#include "blockcache/store/cache_store.h"
#include "blockcache/store/layout.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {

class DiskCacheLoader {
 public:
  DiskCacheLoader(const DiskCacheLayout& layout, CacheManager* manager);

  DiskCacheLoader(const DiskCacheLoader&) = delete;
  DiskCacheLoader& operator=(const DiskCacheLoader&) = delete;

  Future<> Start(UploadFunc uploader);
  Future<> Shutdown();

  bool IsLoading() const { return loading_; }

 private:
  static constexpr size_t kBatchSize = 32;

  struct Result {
    Result& operator+=(const Result& o) {
      loaded += o.loaded;
      removed += o.removed;
      elapsed_ns += o.elapsed_ns;
      return *this;
    }

    uint64_t loaded = 0;
    uint64_t removed = 0;
    uint64_t elapsed_ns = 0;
  };

  Future<> LoadAllBlocks();
  Future<> LoadStageBlocks();
  Future<> LoadCacheBlocks();
  Future<Result> LoadDir(std::string dir, uint64_t fs_id, bool staged);
  Future<> LoadBlock(std::string path, BlockHandle handle, bool staged,
                     Result* result);
  Future<> Unlink(std::string path, Result* result);

  static std::string_view Filename(const std::string& path);
  static uint32_t SteadyAtime(int64_t atime_sec);

  bool running_ = false;
  bool loading_ = false;
  UploadFunc uploader_;
  DiskCacheLayout layout_;
  CacheManager* manager_;
  Gate gate_;
};

using DiskCacheLoaderUPtr = std::unique_ptr<DiskCacheLoader>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_DISK_CACHE_LOADER_H_
