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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_LOCAL_FILESYSTEM_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_LOCAL_FILESYSTEM_H_

#include <dirent.h>
#include <fcntl.h>
#include <sys/vfs.h>

#include <functional>
#include <memory>
#include <string>

#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/blockcache/disk_state_machine_impl.h"
#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::time::TimeSpec;

class PosixFileSystem {
 public:
  PosixFileSystem(std::shared_ptr<DiskStateMachine> disk_state_machine);

  ~PosixFileSystem();

  BCACHE_ERROR Stat(const std::string& path, struct stat* stat);

  BCACHE_ERROR MkDir(const std::string& path, uint16_t mode);

  BCACHE_ERROR OpenDir(const std::string& path, ::DIR** dir);

  BCACHE_ERROR ReadDir(::DIR* dir, struct dirent** dirent);

  BCACHE_ERROR CloseDir(::DIR* dir);

  BCACHE_ERROR Create(const std::string& path, int* fd);

  BCACHE_ERROR Open(const std::string& path, int flags, int* fd);

  BCACHE_ERROR LSeek(int fd, off_t offset, int whence);

  BCACHE_ERROR Write(int fd, const char* buffer, size_t count);

  BCACHE_ERROR Read(int fd, char* buffer, size_t count);

  BCACHE_ERROR Close(int fd);

  BCACHE_ERROR Unlink(const std::string& path);

  BCACHE_ERROR Link(const std::string& oldpath, const std::string& newpath);

  BCACHE_ERROR Rename(const std::string& oldpath, const std::string& newpath);

  BCACHE_ERROR StatFS(const std::string& path, struct statfs* statfs);

 private:
  template <typename... Args>
  BCACHE_ERROR PosixError(int code, const char* format, const Args&... args);

  void CheckError(BCACHE_ERROR rc);

 private:
  std::shared_ptr<DiskStateMachine> disk_state_machine_;
};

// The local filesystem with high-level utilities for block cache
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

  using WalkFunc = std::function<BCACHE_ERROR(const std::string& prefix,
                                              const FileInfo& info)>;

  using DoFunc = std::function<BCACHE_ERROR(
      const std::shared_ptr<PosixFileSystem>& posix)>;

 public:
  explicit LocalFileSystem(
      std::shared_ptr<DiskStateMachine> disk_state_machine = nullptr);

  ~LocalFileSystem() = default;

  BCACHE_ERROR MkDirs(const std::string& path);

  // NOTE: only invoke WalkFunc for file
  BCACHE_ERROR Walk(const std::string& prefix, WalkFunc func);

  BCACHE_ERROR WriteFile(const std::string& path, const char* buffer,
                         size_t count);

  BCACHE_ERROR ReadFile(const std::string& path, std::shared_ptr<char>& buffer,
                        size_t* length);

  BCACHE_ERROR RemoveFile(const std::string& path);

  BCACHE_ERROR HardLink(const std::string& oldpath, const std::string& newpath);

  bool FileExists(const std::string& path);

  BCACHE_ERROR GetDiskUsage(const std::string& path, struct StatDisk* stat);

  BCACHE_ERROR Do(DoFunc func);

 private:
  std::shared_ptr<PosixFileSystem> posix_;
};

std::shared_ptr<LocalFileSystem> NewTempLocalFileSystem();

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_LOCAL_FILESYSTEM_H_
