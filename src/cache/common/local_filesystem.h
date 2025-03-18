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

#ifndef DINGOFS_SRC_CACHE_COMMON_LOCAL_FILESYSTEM_H_
#define DINGOFS_SRC_CACHE_COMMON_LOCAL_FILESYSTEM_H_

#include <dirent.h>
#include <fcntl.h>
#include <sys/vfs.h>

#include <memory>
#include <string>

#include "base/time/time.h"
#include "cache/blockcache/disk_state_machine_impl.h"
#include "cache/common/errno.h"
#include "cache/common/posix.h"

#define IO_ALIGNED_BLOCK_SIZE 4096

namespace dingofs {
namespace cache {
namespace common {

using base::time::TimeSpec;

// The local filesystem with high-level utilities
class LocalFileSystem {
 public:
  struct StatDisk {
    StatDisk() = default;

    uint64_t total_bytes;
    uint64_t total_files;
    uint64_t free_bytes;
    uint64_t free_files;
    double free_bytes_ratio;
    double free_files_ratio;
  };

  struct FileInfo {
    FileInfo(const std::string& name, size_t size, TimeSpec atime)
        : name(name), size(size), atime(atime) {}

    std::string name;
    size_t size;
    TimeSpec atime;
  };

  using WalkFunc =
      std::function<Errno(const std::string& prefix, const FileInfo& info)>;

  using DoFunc = std::function<Errno()>;

  using CheckErrFunc = std::function<Errno(Errno)>;

 public:
  explicit LocalFileSystem(CheckErrFunc check_err_func = nullptr);

  ~LocalFileSystem() = default;

  Errno MkDirs(const std::string& path);

  // NOTE: only invoke WalkFunc for file
  Errno Walk(const std::string& prefix, WalkFunc func);

  Errno WriteFile(const std::string& path, const char* buffer, size_t length,
                  bool use_direct = false);

  Errno ReadFile(const std::string& path, std::shared_ptr<char>& buffer,
                 size_t* length, bool drop_page_cache = false);

  Errno RemoveFile(const std::string& path);

  Errno HardLink(const std::string& oldpath, const std::string& newpath);

  bool FileExists(const std::string& path);

  Errno GetDiskUsage(const std::string& path, struct StatDisk* stat);

  Errno Do(DoFunc func);

 private:
  Errno CheckErr(Errno rc);

  bool IsAligned(uint64_t n);

 private:
  CheckErrFunc check_err_func_;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_LOCAL_FILESYSTEM_H_
