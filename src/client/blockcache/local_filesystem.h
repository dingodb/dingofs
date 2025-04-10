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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_LOCAL_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_LOCAL_FILESYSTEM_H_

#include <dirent.h>
#include <fcntl.h>
#include <sys/vfs.h>

#include <functional>
#include <memory>
#include <string>

#include "base/time/time.h"
#include "client/blockcache/disk_state_machine_impl.h"
#include "client/common/status.h"

#define IO_ALIGNED_BLOCK_SIZE 4096

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::base::time::TimeSpec;

class PosixFileSystem {
 public:
  PosixFileSystem(std::shared_ptr<DiskStateMachine> disk_state_machine);

  ~PosixFileSystem() = default;

  Status Stat(const std::string& path, struct stat* stat);

  Status MkDir(const std::string& path, uint16_t mode);

  Status OpenDir(const std::string& path, ::DIR** dir);

  Status ReadDir(::DIR* dir, struct dirent** dirent);

  Status CloseDir(::DIR* dir);

  Status Create(const std::string& path, int* fd, bool use_direct);

  Status Open(const std::string& path, int flags, int* fd);

  Status LSeek(int fd, off_t offset, int whence);

  Status Write(int fd, const char* buffer, size_t length);

  Status Read(int fd, char* buffer, size_t length);

  Status Close(int fd);

  Status Unlink(const std::string& path);

  Status Link(const std::string& oldpath, const std::string& newpath);

  Status Rename(const std::string& oldpath, const std::string& newpath);

  Status StatFS(const std::string& path, struct statfs* statfs);

  Status FAdvise(int fd, int advise);

 private:
  template <typename... Args>
  Status PosixError(int code, const char* format, const Args&... args);

  void CheckError(Status status);

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

  using WalkFunc =
      std::function<Status(const std::string& prefix, const FileInfo& info)>;

  using DoFunc =
      std::function<Status(const std::shared_ptr<PosixFileSystem>& posix)>;

 public:
  explicit LocalFileSystem(
      std::shared_ptr<DiskStateMachine> disk_state_machine = nullptr);

  ~LocalFileSystem() = default;

  Status MkDirs(const std::string& path);

  // NOTE: only invoke WalkFunc for file
  Status Walk(const std::string& prefix, WalkFunc func);

  Status WriteFile(const std::string& path, const char* buffer, size_t length,
                   bool use_direct = false);

  Status ReadFile(const std::string& path, std::shared_ptr<char>& buffer,
                  size_t* length, bool drop_page_cache = false);

  Status RemoveFile(const std::string& path);

  Status HardLink(const std::string& oldpath, const std::string& newpath);

  bool FileExists(const std::string& path);

  Status GetDiskUsage(const std::string& path, struct StatDisk* stat);

  Status Do(DoFunc func);

 private:
  bool IsAligned(uint64_t n);

 private:
  std::shared_ptr<PosixFileSystem> posix_;
};

std::shared_ptr<LocalFileSystem> NewTempLocalFileSystem();

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_LOCAL_FILESYSTEM_H_
