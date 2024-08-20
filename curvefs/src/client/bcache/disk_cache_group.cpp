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

#include <map>

#include "curvefs/src/client/bcache/disk_cache_group.h"

namespace curvefs {
namespace client {
namespace bcache {

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : options_(options) {}

BCACHE_ERROR DiskCacheGroup::Init(UploadFunc uploader) {
    for (const auto& option : options_) {
        auto store = std::make_shared<DiskCache>(option);
        auto rc = store->Init(uploader);
        if (rc != BCACHE_ERROR::OK) {
            return rc;
        }
        stores_.emplace_back(store);
        // stores_[store->Id()] = store; TODO: chash
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheGroup::Shutdown() {
    for (const auto& it : stores_) {
        auto rc = it.second->Shutdown();
        if (rc != BCACHE_ERROR::OK) {
            return rc;
        }
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheGroup::Stage(const BlockKey& key, const Block& block) {
    return GetStore(key)->Stage(key, block);
}

BCACHE_ERROR DiskCacheGroup::RemoveStage(const BlockKey& key) {
    return GetStore(key)->RemoveStage(key);
}

BCACHE_ERROR DiskCacheGroup::Cache(const BlockKey& key, const Block& block) {
    return GetStore(key)->Cache(key, block);
}

BCACHE_ERROR DiskCacheGroup::Load(const BlockKey& key, Block* block) {
    return GetStore(key)->Load(key, block);
}

std::shared_ptr<DiskCache> DiskCacheGroup::GetStore(const BlockKey& key) {
    return stores_[key.id % stores_.size()];  // TODO(Wine93): chash
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
