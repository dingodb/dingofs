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

#ifndef DINGOFS_SRC_CACHE_UTILS_POSIX_H_
#define DINGOFS_SRC_CACHE_UTILS_POSIX_H_

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/vfs.h>

#include <functional>
#include <memory>
#include <string>

#include "cache/common/common.h"

namespace dingofs {
namespace cache {

// Wrapper for POSIX interface which does:
//  1. convert system error code to Status
//  2. add logging
class Posix {
 public:
  static Status Stat(const std::string& path, struct stat* stat);

  static Status MkDir(const std::string& path, uint16_t mode);

  static Status OpenDir(const std::string& path, ::DIR** dir);

  static Status ReadDir(::DIR* dir, struct dirent** dirent);

  static Status CloseDir(::DIR* dir);

  static Status Open(const std::string& path, int flags, int* fd);

  static Status Open(const std::string& path, int flags, mode_t mode, int* fd);

  static Status Creat(const std::string& path, mode_t mode, int* fd);

  static Status LSeek(int fd, off_t offset, int whence);

  static Status Write(int fd, const char* buffer, size_t length);

  static Status Read(int fd, char* buffer, size_t length);

  static Status Close(int fd);

  static Status Unlink(const std::string& path);

  static Status Link(const std::string& from, const std::string& to);

  static Status Rename(const std::string& oldpath, const std::string& newpath);

  static Status StatFS(const std::string& path, struct statfs* statfs);

  static Status PosixFAdvise(int fd, off_t offset, size_t length, int advise);

  static Status MMap(void* addr, size_t length, int port, int flags, int fd,
                     off_t offset, void** addr_out);

  static Status MUnMap(void* addr, size_t length);

 private:
  template <typename... Args>
  static Status PosixError(int code, const char* format, const Args&... args);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_POSIX_H_
