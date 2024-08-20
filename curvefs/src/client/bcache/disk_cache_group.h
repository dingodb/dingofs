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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_GROUP_H_
#define CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_GROUP_H_

#include <string>
#include <vector>

#include "curvefs/src/client/bcache/cache_store.h"

namespace curvefs {
namespace client {
namespace bcache {

class DiskCacheGroup : public CacheStore {
 public:
    explicit DiskCacheGroup(std::vector<DiskCacheOption> options);

    BCACHE_ERROR Init(UploadFunc uploader) override;

    BCACHE_ERROR Shutdown() override;

    BCACHE_ERROR Stage(const BlockKey& key, const Block& block) override;

    BCACHE_ERROR RemoveStage(const BlockKey& key) override;

    BCACHE_ERROR Cache(const BlockKey& key, const Block& block) override;

    BCACHE_ERROR Load(const BlockKey& key, Block* block) override;

  	// BCACHE_ERROR Stat(CacheStat* stat) override;
    // bool IsFull() override;
    // bool IsCached(const BlockKey& key) override;

 private:
    std::shared_ptr<DiskCache> GetStore(const BlockKey& key);

 private:
    std::vector<DiskCacheOption> options_;
    std::vector<std::shared_ptr<DiskCache>> stores_;

    // TODO: chash
    // std::unordered_map<std::string, std::shared_ptr<DiskCache>> stores_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_GROUP_H_
