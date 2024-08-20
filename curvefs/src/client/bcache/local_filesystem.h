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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_LOCAL_FILESYSTEM_H_
#define CURVEFS_SRC_CLIENT_BCACHE_LOCAL_FILESYSTEM_H_

#include <atomic>
#include <string>
#include <functional>

#include "src/common/throttle.h"
#include "curvefs/src/base/time.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curve::common::Throttle;
using ::curvefs::base::time::TimeSpec;

// The local filesystem high-level utilities, for block cache,
// inspired by https://github.com/spf13/afero
class LocalFileSystem {
 public:
    struct FileInfo {
        FileInfo(const std::string& name, size_t size, TimeSpec atime)
            : name(name), size(size), atime(atime) {}

        std::string name;
        size_t size;
        TimeSpec atime;
    };

    struct StatFS {
        uint64_t totalBytes;
        uint64_t totalFiles;
        uint64_t freeBytes;
        uint64_t freeFiles;
        double freeBytesRatio;
        double freeFilesRatio;
    };

    using WalkFunc = std::function<BCACHE_ERROR(const std::string prefix,
                                                const FileInfo& info)>;

 public:
    LocalFileSystem();

    ~LocalFileSystem() = default;

    BCACHE_ERROR Walk(const std::string& root, WalkFunc func);

    BCACHE_ERROR Mkdir(const std::string& path, uint16_t mode);

    BCACHE_ERROR Mkdirs(const std::string& path, uint16_t mode);

    BCACHE_ERROR WriteFile(const std::string& path,
                           const char* buffer,
                           size_t count);

    BCACHE_ERROR ReadFile(const std::string& path,
                          char* buffer,
                          size_t* count);

    BCACHE_ERROR RemoveFile(const std::string& path);

    BCACHE_ERROR Link(const std::string& oldpath, const std::string& newpath);

    BCACHE_ERROR StatFS(const std::string& path, StatFS* statfs);

 private:
    BCACHE_ERROR ToBcacheError(int code);

 private:
    std::shared_ptr<Throttle> throttle_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_LOCAL_FILESYSTEM_H_
