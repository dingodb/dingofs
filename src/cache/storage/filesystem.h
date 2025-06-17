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

#ifndef DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_H_
#define DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_H_

#include <dirent.h>
#include <fcntl.h>
#include <sys/vfs.h>

#include <functional>
#include <memory>
#include <string>

#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "utils/time.h"

namespace dingofs {
namespace cache {

struct FileInfo {
  FileInfo() = default;

  FileInfo(const std::string& name, uint32_t nlink, size_t size,
           utils::TimeSpec atime)
      : name(name), nlink(nlink), size(size), atime(atime) {}

  std::string name;
  uint32_t nlink;
  size_t size;
  utils::TimeSpec atime;
};

using WalkFunc =
    std::function<Status(const std::string& prefix, const FileInfo& info)>;

struct FSStat {
  uint64_t total_bytes{0};
  uint64_t total_files{0};
  uint64_t free_bytes{0};
  uint64_t free_files{0};
  double free_bytes_ratio{0};
  double free_files_ratio{0};
};

struct WriteOption {
  bool drop_page_cache{false};
};

struct ReadOption {
  bool drop_page_cache{false};
};

// The filesystem with high-level utilities.
class FileSystem {
 public:
  virtual ~FileSystem() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status MkDirs(const std::string& path) = 0;

  // NOTE: only invoke WalkFunc for file
  virtual Status Walk(const std::string& prefix, WalkFunc func) = 0;

  virtual Status WriteFile(ContextSPtr ctx, const std::string& path,
                           const IOBuffer& buffer,
                           WriteOption option = WriteOption()) = 0;

  virtual Status ReadFile(ContextSPtr ctx, const std::string& path,
                          off_t offset, size_t length, IOBuffer* buffer,
                          ReadOption option = ReadOption()) = 0;

  virtual Status RemoveFile(const std::string& path) = 0;

  virtual Status Link(const std::string& from, const std::string& to) = 0;

  virtual bool FileExists(const std::string& path) = 0;

  virtual Status StatFile(const std::string& path, FileInfo* info) = 0;

  virtual Status StatFS(const std::string& path, FSStat* stat) = 0;
};

using FileSystemSPtr = std::shared_ptr<FileSystem>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_H_
