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

#include <glog/logging.h>
#include <map>

#include "curvefs/src/common/threading.h"
#include "curvefs/src/client/bcache/disk_cache.h"

namespace curvefs {
namespace client {
namespace bcache {

DiskCache::DiskCache(DiskCacheOption option)
    : cacheDir_(option.cacheDir)
      option_(option) {
    fs_ = std::shared_ptr<LocalFileSystem>();
    layout_ = std::shared_ptr<DiskCacheLayout>(cacheDir_);

    cache_ = std::make_shared<DiskCacheManager>(
        option_.cacheDir, option_.cacheSize, option_.spaceFreeRatio, handler);
    loader_ = std::make_unique<DiskCacheLoader>(option_.cacheDir, cache_);
}

BCACHE_ERROR DiskCache::Init(UploadFunc uploader) {
    uploader_ = uploader;
    cache_->Start();
    loader_->Start();
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCache::LoadMetaFile(std::string* id) {
    bool exist;
    std::string filepath = StrFormat("%s/.meta", cacheDir_);
    auto succ = fs_->Exists(filepath, &exist);
    if (!succ) {
        return BCACHE_ERROR::FAIL;
    } else if (exist) {
        succ = fs_->ReadFile(filepath, uuid.c_str())
    } else {
        succ = fs_->WriteFile(filepath)
    }
    return succ ? BCACHE_ERROR::OK : BCACHE_ERROR::FAIL;
}

BCACHE_ERROR DiskCache::Shutdown() {
    // status_ = CACHE_STATUS::SHUTTING_DOWN;
    loader_->Stop();
    cache_->Stop();
    // status_ = CACHE_STATUS::SHUTTING_DOWN;
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCache::Stage(const BlockKey& key, const Block& block) {
    auto rc = Check(WANT_EXEC | WANT_STAGE);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    std::string filepath = GetStagePath(key);
    rc = fs_->WriteFile(filepath, block.data, block.size);
    if (rc == BCACHE_ERROR::OK) {
        rc = fs_->HardLink(filepath, GetCachePath(key));
    }

    succ = fs_->HardLink(filepath, GetCachePath(key));
    if (succ) {
        cache_->Add(key, block.size, Now());
    } else {
        LOG(WARN) << "link " << filepath << " to "
                  << GetCachePath(key) << " failed";
    }
    uploader_(key, filepath);
    return succ ? BCACHE_ERROR::OK : BCACHE_ERROR::FAIL;
}

BCACHE_ERROR DiskCache::RemoveStage(const BlockKey& key) {
    BCACHE_ERROR rc;

    rc = Check(WANT_EXEC);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    std::string filepath = GetStagePath(key);
    rc = fs_->RemoveFile(filepath);
    return rc;
}

BCACHE_ERROR DiskCache::Cache(const BlockKey& key, const Block& block) {
    BCACHE_ERROR rc;

    rc = Check(WANT_EXEC | WANT_CACHE);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    std::string filepath = GetCachePath(key);
    rc = fs_->WriteFile(filepath, block.data, block.size);
    if (rc == BCACHE_ERROR::OK) {
        cache_->Add(key, block.size, TimeNow());
    }
    return rc;
}

BCACHE_ERROR DiskCache::Load(const BlockKey& key, Block* block) {
    BCACHE_ERROR rc;

    rc = Check(WANT_EXEC);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    rc = IsCached(key);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    std::string filepath = GetCachePath(key);
    rc = fs_->ReadFile(filepath, block.data, &block.size);
    if (rc == BCACHE_ERROR::OK) {
        cache_->Access(key);
    }
    return rc;
}

// inline CACHE_STATUS DiskCache::Status() const {
//     return status_;
// }

inline bool DiskCache::Loading() const {
    return loader_.IsLoading();
}

inline bool DiskCache::Healthy() const {
    return true;
}

inline bool DiskCache::StageFull() const {
    return cache_.StageFull();
}

inline bool DiskCache::CacheFull() const {
    return cache_.CacheFull();
}

inline std::string DiskCache::GetStagePath(const BlockKey& key) const {
    return layout_->GetStagePath(key);
}

inline std::string DiskCache::GetCachePath(const BlockKey& key) const {
    return layout_->GetCachePath(key);
}

BCACHE_ERROR DiskCache::IsCached(const BlockKey& key) const {
    if (cache_->Exists(key)) {
        return true;
    } else if (Loading() && fs_->Exists(GetCachePath(key))) {
        return true;
    }
    return false;
}

BCACHE_ERROR DiskCache::Check(uint16_t want) {
    auto status = Status();
    switch (status) {
    case CacheStatus::UP:
        break;
    case CacheStatus::SHUTTING_DOWN:
        return BCACHE_ERROR::SHUTTING_DOWN;
    case CacheStatus::DOWN:
        return BCACHE_ERROR::DOWN;
    default:
        break;
    }

    if ((want & WANT_EXEC) && !Healthy()) {
        return BCACHE_ERROR::UNHEALTHY;
    } else if ((want & WANT_STAGE) && StageFull()) {
        return BCACHE_ERROR::FULL;
    } else if ((want & WANT_CACHE) && CacheFull()) {
        return BCACHE_ERROR::FULL;
    }
    return BCACHE_ERROR::OK;
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
