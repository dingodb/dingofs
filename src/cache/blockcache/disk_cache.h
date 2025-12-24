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

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_loader.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "cache/blockcache/local_filesystem.h"
#include "common/const.h"

namespace dingofs {
namespace cache {

struct DiskCacheVarsCollector {
  DiskCacheVarsCollector(uint64_t cache_index, const std::string& cache_dir,
                         uint64_t cache_size_mb, double free_space_ratio)
      : cache_index(cache_index),
        prefix(absl::StrFormat("dingofs_disk_cache_%d", cache_index)),
        uuid(Name("uuid"), "-"),
        dir(Name("dir"), cache_dir.c_str()),
        capacity(Name("capacity"), cache_size_mb * kMiB),
        free_space_ratio(Name("free_space_ratio"), free_space_ratio),
        running_status(Name("running_status"), "down"),
        stage_skips(Name("stage_skips")),  // stage
        cache_hits(Name("cache_hits")),    // cache
        cache_misses(Name("cache_misses")) {}

  std::string Name(const std::string& name) const {
    CHECK_GT(prefix.length(), 0);
    return absl::StrFormat("%s_%s", prefix, name);
  }

  void Reset() {
    running_status.set_value("down");
    stage_skips.reset();
    cache_hits.reset();
    cache_misses.reset();
  }

  uint64_t cache_index;
  std::string prefix;
  bvar::Status<std::string> uuid;
  bvar::Status<std::string> dir;
  bvar::Status<int64_t> capacity;
  bvar::Status<double> free_space_ratio;
  bvar::Status<std::string> running_status;
  bvar::Adder<int64_t> stage_skips;  // stage
  bvar::Adder<int64_t> cache_hits;   // cache
  bvar::Adder<int64_t> cache_misses;
};

using DiskCacheVarsCollectorUPtr = std::unique_ptr<DiskCacheVarsCollector>;

struct DiskCacheVarsRecordGuard {
  DiskCacheVarsRecordGuard(const std::string& op_name, Status& status,
                           DiskCacheVarsCollector* vars)
      : status(status), op_name(op_name), vars(vars) {}

  ~DiskCacheVarsRecordGuard() {
    if (op_name == "Load") {
      if (status.ok()) {
        vars->cache_hits << 1;
      } else {
        vars->cache_misses << 1;
      }
    } else if (op_name == "Stage") {
      if (!status.ok()) {
        vars->stage_skips << 1;
      }
    }
  }

  std::string op_name;
  Status& status;
  DiskCacheVarsCollector* vars;
};

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

  std::string Id() const override { return uuid_; }

  bool IsRunning() const override {
    return running_.load(std::memory_order_relaxed);
  }

  bool IsCached(const BlockKey& key) const override {
    return manager_->Exist(key) ||
           (StillLoading() && iutil::FileIsExist(GetCachePath(key)));
  }

  bool IsFull(const BlockKey&) const override { return CacheFull(); }

 private:
  friend class Target;

  enum WantType : uint8_t {
    kWantExec = 1,
    kWantStage = 2,
    kWantCache = 4,
  };

  // for start
  Status CreateDirs();
  Status LoadOrCreateLockFile();
  bool DetectDirectIO();

  // check running status, disk free space
  Status CheckStatus(uint8_t want) const;
  bool StillLoading() const { return loader_->StillLoading(); }
  bool StageFull() const { return manager_->StageFull(); }
  bool CacheFull() const { return manager_->CacheFull(); }

  // path utility
  std::string GetRootDir() const { return layout_->GetRootDir(); }
  std::string GetStageDir() const { return layout_->GetStageDir(); }
  std::string GetCacheDir() const { return layout_->GetCacheDir(); }
  std::string GetProbeDir() const { return layout_->GetProbeDir(); }
  std::string GetDetectPath() const { return layout_->GetDetectPath(); }
  std::string GetLockPath() const { return layout_->GetLockPath(); }

  std::string GetStagePath(const BlockKey& key) const {
    return layout_->GetStagePath(key);
  }

  std::string GetCachePath(const BlockKey& key) const {
    return layout_->GetCachePath(key);
  }

 private:
  std::atomic<bool> running_;
  UploadFunc uploader_;
  std::string uuid_;
  DiskCacheLayoutSPtr layout_;
  LocalFileSystemUPtr localfs_;
  DiskCacheManagerSPtr manager_;
  DiskCacheLoaderUPtr loader_;
  DiskCacheVarsCollectorUPtr vars_;
};

using DiskCacheSPtr = std::shared_ptr<DiskCache>;
using DiskCacheUPtr = std::unique_ptr<DiskCache>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_H_
