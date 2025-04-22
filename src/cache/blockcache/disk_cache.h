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
#include "cache/common/aio_queue.h"
#include "cache/common/local_filesystem.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using cache::common::AioQueueImpl;

class DiskCache : public CacheStore {
  enum : std::int8_t {
    kWantExec = 1,
    kWantStage = 2,
    kWantCache = 4,
  };

 public:
  ~DiskCache() override = default;

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
  // for init
  Status CreateDirs();

  Status LoadLockFile();

  void DetectDirectIO();

  // for read
  Status NewBlockReader(const BlockKey& key,
                        std::shared_ptr<BlockReader>& reader);

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
  std::shared_ptr<AioQueueImpl> aio_queue_;
  bool use_direct_write_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_H_
