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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_H_

#include <atomic>
#include <memory>
#include <string>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_loader.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "cache/blockcache/disk_cache_metric.h"
#include "cache/blockcache/disk_state_health_checker.h"
#include "cache/blockcache/disk_state_machine.h"
#include "cache/blockcache/page_cache_manager.h"
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/common/io_buffer.h"
#include "cache/common/local_filesystem.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using cache::common::DiskCacheOptions;
using cache::common::LocalFileSystem;

class BlockReaderImpl : public BlockReader {
 public:
  BlockReaderImpl(int fd, std::shared_ptr<LocalFileSystem> fs);

  ~BlockReaderImpl() override = default;

  Errno ReadAt(off_t offset, size_t length, char* buffer) override;

  void Close() override;

 private:
  int fd_;
  std::shared_ptr<LocalFileSystem> fs_;
};

class DiskCache : public CacheStore {
  enum : uint8_t {
    WANT_EXEC = 1,
    WANT_STAGE = 2,
    WANT_CACHE = 4,
  };

 public:
  ~DiskCache() override = default;

  explicit DiskCache(DiskCacheOptions option);

  Errno Init(UploadFunc uploader) override;

  Errno Shutdown() override;

  Errno Stage(const BlockKey& key, const Block& block,
              BlockContext ctx) override;

  Errno RemoveStage(const BlockKey& key, BlockContext ctx) override;

  Errno Cache(const BlockKey& key, const Block& block) override;

  Errno Load(const BlockKey& key,
             std::shared_ptr<BlockReader>& reader) override;

  Errno Load(const BlockKey& key, uint64_t offset, uint64_t size,
             IOBuffer* buffer) override;

  bool IsCached(const BlockKey& key) override;

  std::string Id() override;

 private:
  Errno CreateDirs();

  Errno LoadLockFile();

  void DetectDirectIO();

  // check running status, disk healthy and disk free space
  Errno Check(uint8_t want);

  bool IsLoading() const;

  bool IsHealthy() const;

  bool StageFull() const;

  bool CacheFull() const;

  std::string GetRootDir() const;

  std::string GetStagePath(const BlockKey& key) const;

  std::string GetCachePath(const BlockKey& key) const;

 private:
  std::string uuid_;
  UploadFunc uploader_;
  DiskCacheOptions options_;
  std::atomic<bool> running_;
  std::shared_ptr<DiskCacheMetric> metric_;
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<DiskStateMachine> disk_state_machine_;
  std::unique_ptr<DiskStateHealthChecker> disk_state_health_checker_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::shared_ptr<DiskCacheManager> manager_;
  std::unique_ptr<DiskCacheLoader> loader_;
  std::unique_ptr<PageCacheManager> page_cache_manager_;
  bool use_direct_write_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_H_
