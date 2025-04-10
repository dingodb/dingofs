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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_H_

#include <atomic>
#include <memory>
#include <string>

#include "client/blockcache/cache_store.h"
#include "client/blockcache/disk_cache_layout.h"
#include "client/blockcache/disk_cache_loader.h"
#include "client/blockcache/disk_cache_manager.h"
#include "client/blockcache/disk_cache_metric.h"
#include "client/blockcache/disk_state_health_checker.h"
#include "client/blockcache/disk_state_machine.h"
#include "client/blockcache/local_filesystem.h"
#include "client/common/config.h"
#include "client/common/status.h"

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::client::common::DiskCacheOption;

class BlockReaderImpl : public BlockReader {
 public:
  BlockReaderImpl(int fd, std::shared_ptr<LocalFileSystem> fs);

  virtual ~BlockReaderImpl() = default;

  Status ReadAt(off_t offset, size_t length, char* buffer) override;

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
  virtual ~DiskCache() = default;

  explicit DiskCache(DiskCacheOption option);

  Status Init(UploadFunc uploader) override;

  Status Shutdown() override;

  Status Stage(const BlockKey& key, const Block& block,
               BlockContext ctx) override;

  Status RemoveStage(const BlockKey& key, BlockContext ctx) override;

  Status Cache(const BlockKey& key, const Block& block) override;

  Status Load(const BlockKey& key,
              std::shared_ptr<BlockReader>& reader) override;

  bool IsCached(const BlockKey& key) override;

  std::string Id() override;

 private:
  Status CreateDirs();

  Status LoadLockFile();

  void DetectDirectIO();

  // check running status, disk healthy and disk free space
  Status Check(uint8_t want);

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
  DiskCacheOption option_;
  std::atomic<bool> running_;
  std::shared_ptr<DiskCacheMetric> metric_;
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<DiskStateMachine> disk_state_machine_;
  std::unique_ptr<DiskStateHealthChecker> disk_state_health_checker_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::shared_ptr<DiskCacheManager> manager_;
  std::unique_ptr<DiskCacheLoader> loader_;
  bool use_direct_write_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_H_
