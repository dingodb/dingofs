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

#ifndef DINGOFS_SRC_CACHE_COMMON_POSIX_H_
#define DINGOFS_SRC_CACHE_COMMON_POSIX_H_

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/vfs.h>

#include <string>

#include "cache/common/errno.h"

namespace dingofs {
namespace cache {
namespace common {

// Wrapper for posix interface
class Posix {
 public:
  static Errno Stat(const std::string& path, struct stat* stat);

  static Errno MkDir(const std::string& path, uint16_t mode);

  static Errno OpenDir(const std::string& path, ::DIR** dir);

  static Errno ReadDir(::DIR* dir, struct dirent** dirent);

  static Errno CloseDir(::DIR* dir);

  static Errno Create(const std::string& path, int* fd, bool use_direct);

  static Errno Open(const std::string& path, int flags, mode_t mode, int* fd);

  static Errno LSeek(int fd, off_t offset, int whence);

  static Errno Write(int fd, const char* buffer, size_t length);

  static Errno Read(int fd, char* buffer, size_t length);

  static Errno Close(int fd);

  static Errno Unlink(const std::string& path);

  static Errno Link(const std::string& oldpath, const std::string& newpath);

  static Errno Rename(const std::string& oldpath, const std::string& newpath);

  static Errno StatFS(const std::string& path, struct statfs* statfs);

  static Errno PosixFAdvise(int fd, off_t offset, size_t length, int advise);

  static Errno MMap(void* addr, size_t length, int port, int flags, int fd,
                    off_t offset, void** addr_out);

  static Errno MUnmap(void* addr, size_t length);

 private:
  template <typename... Args>
  static Errno PosixError(int code, const std::string& format,
                          const Args&... args);
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_POSIX_H_
