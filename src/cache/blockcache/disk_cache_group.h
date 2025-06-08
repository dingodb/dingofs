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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_

#include "base/hash/ketama_con_hash.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_cache_watcher.h"

namespace dingofs {
namespace cache {

class DiskCacheGroup final : public CacheStore {
 public:
  explicit DiskCacheGroup(std::vector<DiskCacheOption> options);
  ~DiskCacheGroup() override = default;

  Status Init(UploadFunc uploader) override;
  Status Shutdown() override;

  Status Stage(const BlockKey& key, const Block& block,
               StageOption option) override;
  Status RemoveStage(const BlockKey& key, RemoveStageOption option) override;
  Status Cache(const BlockKey& key, const Block& block,
               CacheOption option) override;
  Status Load(const BlockKey& key, off_t offset, size_t length,
              IOBuffer* buffer, LoadOption option) override;

  std::string Id() const override;
  bool IsRunning() const override;
  bool IsCached(const BlockKey& key) const override;

 private:
  static std::vector<uint64_t> CalcWeights(
      std::vector<DiskCacheOption> options);
  DiskCacheSPtr GetStore(const BlockKey& key) const;
  DiskCacheSPtr GetStore(const std::string& store_id) const;

  std::atomic<bool> running_;
  const std::vector<DiskCacheOption> options_;
  std::unique_ptr<base::hash::ConHash> chash_;
  std::unordered_map<std::string, DiskCacheSPtr> stores_;
  DiskCacheWatcherUPtr watcher_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_
