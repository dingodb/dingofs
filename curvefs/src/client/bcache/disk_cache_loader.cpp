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
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#include <map>
#include <string>

#include "curvefs/src/client/bcache/disk_cache_loader.h"

namespace curvefs {
namespace client {
namespace bcache {

DiskCacheLoader(const std::string stageDir,
                const std::string cacheDir,
                UploadFunc uploader,
                std::shared_ptr<LocalFileSystem> fs,
                std::shared_ptr<DiskCacheLayout> layout,
                std::shared_ptr<DiskCacheManager> cache)
    : stageDir_(stageDir),
      cacheDir_(cacheDir),
      uploader_(uploader),
      fs_(fs),
      cache_(cache) {}

BCACHE_ERROR DiskCacheLoader::Start() {
    if (!running_.exchange(true)) {
        thread_ = std::thread(&DiskCacheLoader::Load, this);
        LOG(INFO) << "Cache loading thread start success.";
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheLoader::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "Stop cache loading thread...";
        thread_join();
        LOG(INFO) << "Cache loading thread stopped.";
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheLoader::LoadStage(const std::string& root) {
    BlockKey key;
    uint64_t count = 0, size = 0;
    auto rc = fs_->Walk(root, [this](const std::string& prefix,
                                     const LocalFileSystem::FileInfo& info) {
        if (!running_) {
            return BCACHE_ERROR::ABORT;
        } else if (Validate(prefix, info.name, &key)) {
            uploader_(key, PathJoin(root, info.name));
            count++;
            size += info.size;
        }
        return BCACHE_ERROR::OK;
    })
    LOG(INFO) << "[stage_loader] load " << StrErr(rc)
              << ": " << count << " block stage files loaded"
              << ", total bytes = " << size;
}

BCACHE_ERROR DiskCacheLoader::LoadCache(const std::string& root) {
    BlockKey key;
    uint64_t count = 0, size = 0;
    auto rc = fs_->Walk(root, [this](const std::string& prefix,
                                     const LocalFileSystem::FileInfo& info) {
        if (!running_) {
            return BCACHE_ERROR::ABORT;
        } else if (Validate(prefix, info.name, &key)) {
            cache_->Add(key, info.size, info.atime);
            count++;
            size += info.size;
        }
        return BCACHE_ERROR::OK;
    })
    LOG(INFO) << "[cache_loader] load " << StrErr(rc)
              << ": " << count << " block cache files loaded"
              << ", total bytes = " << size;
}

bool DiskCacheLoader::Validate(const std::string& prefix,
                               const std::string& name,
                               BlockKey* key) {
    bool valid = true;
    if (HasSuffix(name, ".tmp")) {
        valid = false;
    } else if (!key->ParseFromString(name)) {
        valid = false;
    }

    if (!valid) {
        auto rc = fs_->RemoveFile(PathJoin({ prefix, name }));
        if (rc != BCACHE_ERROR::OK) {
            LOG(WARN) << "Remove invalid file failed: " << StrErr(rc);
        }
    }
    return valid;
}

inline bool DiskCacheLoader::Loading() const {
    return nloading_ == 0;
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
