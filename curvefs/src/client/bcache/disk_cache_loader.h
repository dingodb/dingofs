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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_LOADER_H_
#define CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_LOADER_H_

#include <memory>
#include <string>

#include "curvefs/src/client/bcache/local_filesystem.cpp"
#include "curvefs/src/client/bcache/disk_cache_manager.h"
#include "curvefs/src/client/bcache/disk_cache_layout.h"

namespace curvefs {
namespace client {
namespace bcache {

class DiskCacheLoader {
 public:
    DiskCacheLoader(UploadFunc uploader,
                    std::shared_ptr<LocalFileSystem> fs,
                    std::shared_ptr<DiskCacheLayout> layout,
                    std::shared_ptr<DiskCacheManager> cache);

    BCACHE_ERROR Start();

    BCACHE_ERROR Stop();

    bool Loading() const;

 private:
    BCACHE_ERROR LoadStage(const std::string& root);

    BCACHE_ERROR LoadCache(const std::string& root);

    BCACHE_ERROR Validate(const std::string& prefix,
                          const std::string& name,
                          BlockKey* key);

 public:
    std::thread thread_;
    std::atomic<bool> running_;
    UploadFunc uploader_;
    std::shared_ptr<LocalFileSystem> fs_;
    std::shared_ptr<DiskCacheLayout> layout_;
    std::shared_ptr<DiskCacheManager> cache_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_LOADER_H_