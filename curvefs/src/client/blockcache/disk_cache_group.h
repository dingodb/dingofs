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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_GROUP_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_GROUP_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/src/base/hash/ketama_con_hash.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/disk_cache.h"
#include "curvefs/src/client/blockcache/disk_cache_watcher.h"
#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::hash::ConHash;
using ::curvefs::client::common::DiskCacheOption;
using UploadFunc = CacheStore::UploadFunc;

class DiskCacheGroup : public CacheStore {
 public:
  virtual ~DiskCacheGroup() = default;

  explicit DiskCacheGroup(std::vector<DiskCacheOption> options);

  BCACHE_ERROR Init(UploadFunc uploader) override;

  BCACHE_ERROR Shutdown() override;

  BCACHE_ERROR Stage(const BlockKey& key, const Block& block,
                     BlockContext ctx) override;

  BCACHE_ERROR RemoveStage(const BlockKey& key) override;

  BCACHE_ERROR Cache(const BlockKey& key, const Block& block) override;

  BCACHE_ERROR Load(const BlockKey& key,
                    std::shared_ptr<BlockReader>& reader) override;

  bool IsCached(const BlockKey& key) override;

  std::string Id() override;

 private:
  std::vector<uint64_t> CalcWeights(std::vector<DiskCacheOption> options);

  std::shared_ptr<DiskCache> GetStore(const BlockKey& key);

 private:
  std::vector<DiskCacheOption> options_;
  std::unique_ptr<ConHash> chash_;
  std::unordered_map<std::string, std::shared_ptr<DiskCache>> stores_;
  std::unique_ptr<DiskCacheWatcher> watcher_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_GROUP_H_
