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
 * Created Date: 2024-08-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_LAYOUT_H_
#define CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_LAYOUT_H_

#include "absl/strings/str_format.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::absl::StrFormat;

class DiskCacheLayout {
 public:
    DiskCacheLayout(const std::string& cacheDir)
        : cacheDir_(cacheDir) {}

    std::string GetRootDir() const  {
        return cacheDir_;
    }

    std::string GetMetaPath() const {
        return StrFormat("%s/.meta", cacheDir_);
    }

    // e.g.  x/stage/0/0/1_100_100_100_1
    std::string GetStagePath(const BlockKey& key) const {
        return StrFormat("%s/stage/%s", cacheDir_, key.StoreKey());
    }

    std::string GetCachePath(const BlockKey& key) const {
        return StrFormat("%s/cache/%s", cacheDir_, key.StoreKey());
    }

    std::string GetProbeDir() const {
        return StrFormat("%s/probe", cacheDir_);
    }

 private:
    std::string cacheDir_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_LAYOUT_H_
