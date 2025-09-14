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

#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_loader.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "cache/blockcache/disk_state_health_checker.h"
#include "cache/metric/disk_cache_metric.h"
#include "cache/storage/filesystem.h"

namespace dingofs {
namespace cache {

struct DiskCacheOption {
  DiskCacheOption();

  uint32_t cache_index;
  std::string cache_store;
  std::string cache_dir;
  uint64_t cache_size_mb;
};

class DiskCache final : public CacheStore {
 public:
  explicit DiskCache(DiskCacheOption option);
  ~DiskCache() override = default;

  Status Start(UploadFunc uploader) override;
  Status Shutdown() override;

  Status Stage(ContextSPtr ctx, const BlockKey& key, const Block& block,
               StageOption option = StageOption()) override;
  Status RemoveStage(ContextSPtr ctx, const BlockKey& key,
                     RemoveStageOption option = RemoveStageOption()) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
               CacheOption option = CacheOption()) override;
  Status Load(ContextSPtr ctx, const BlockKey& key, off_t offset, size_t length,
              IOBuffer* buffer, LoadOption option = LoadOption()) override;

  std::string Id() const override;
  bool IsRunning() const override;
  bool IsCached(const BlockKey& key) const override;
  bool IsFull(const BlockKey& key) const override;

 private:
  friend class Target;

  enum : uint8_t {
    kWantExec = 1,
    kWantStage = 2,
    kWantCache = 4,
  };

  // for start
  Status CreateDirs();
  Status LoadOrCreateLockFile();
  bool DetectDirectIO();

  // check running status, disk healthy and disk free space
  Status CheckStatus(uint8_t want) const;
  bool IsLoading() const;
  bool IsHealthy() const;
  bool StageFull() const;
  bool CacheFull() const;

  // path utility
  std::string GetRootDir() const;
  std::string GetStageDir() const;
  std::string GetCacheDir() const;
  std::string GetProbeDir() const;
  std::string GetDetectPath() const;
  std::string GetLockPath() const;
  std::string GetStagePath(const BlockKey& key) const;
  std::string GetCachePath(const BlockKey& key) const;

 private:
  std::atomic<bool> running_;
  UploadFunc uploader_;
  std::string uuid_;
  bool support_direct_io_;
  DiskCacheMetricSPtr metric_;
  DiskCacheLayoutSPtr layout_;
  StateMachineSPtr state_machine_;
  DiskStateHealthCheckerUPtr disk_state_health_checker_;
  FileSystemSPtr fs_;
  DiskCacheManagerSPtr manager_;
  DiskCacheLoaderUPtr loader_;
};

using DiskCacheSPtr = std::shared_ptr<DiskCache>;
using DiskCacheUPtr = std::unique_ptr<DiskCache>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_H_
