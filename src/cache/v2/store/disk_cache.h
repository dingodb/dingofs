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

#ifndef DINGOFS_CACHE_V2_STORE_DISK_CACHE_H_
#define DINGOFS_CACHE_V2_STORE_DISK_CACHE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/v2/common/block_handle.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/store/cache_manager.h"
#include "cache/v2/store/cache_store.h"
#include "cache/v2/store/disk_cache_loader.h"
#include "cache/v2/store/health.h"
#include "cache/v2/store/layout.h"
#include "cache/v2/store/local_filesystem.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

struct DiskOption {
  std::string dir;
  uint64_t capacity_bytes = 0;
};

std::vector<DiskOption> ParseDiskOptions(const std::string& value);

class DiskCache final : public CacheStore {
 public:
  explicit DiskCache(DiskOption option);

  DiskCache(const DiskCache&) = delete;
  DiskCache& operator=(const DiskCache&) = delete;

  Future<> Start(UploadFunc uploader) override;
  Future<> Shutdown() override;

  Future<Status> Stage(BlockHandle handle, BufferViews block) override;
  Future<Status> RemoveStage(BlockHandle handle) override;
  Future<Status> Cache(BlockHandle handle, BufferViews block) override;
  Future<Status> Load(BlockHandle handle, uint64_t offset, uint32_t length,
                      char* buffer) override;
  Future<Status> Delete(BlockHandle handle) override;
  Future<bool> Exists(BlockHandle handle) override;
  Future<CacheStats> GetStats() override;

  const std::string& uuid() const { return uuid_; }
  uint64_t capacity_bytes() const { return option_.capacity_bytes; }

 private:
  friend class RecoveryTest;
  friend class SingleStoreTest;

  enum WantType : uint8_t {
    kWantExec = 1,
    kWantStage = 2,
    kWantCache = 4,
  };

  Status CreateDirs();
  Status GetOrCreateLockFile();
  Status Check(uint8_t want) const;

  std::string GetStagePath(const BlockHandle& handle) const {
    return layout_.StagePath(handle);
  }

  std::string GetCachePath(const BlockHandle& handle) const {
    return layout_.CachePath(handle);
  }

  bool running_ = false;
  const DiskOption option_;
  const DiskCacheLayout layout_;
  std::string uuid_;
  HealthCheckerUPtr health_;
  CacheManagerUPtr manager_;
  DiskCacheLoaderUPtr loader_;
  LocalFileSystemUPtr localfs_;

  uint64_t hits_ = 0;
  uint64_t misses_ = 0;
};

using DiskCacheUPtr = std::unique_ptr<DiskCache>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_STORE_DISK_CACHE_H_
