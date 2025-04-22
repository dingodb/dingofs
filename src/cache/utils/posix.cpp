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

#include "cache/utils/posix.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <memory>

#include "absl/cleanup/cleanup.h"
#include "base/file/file.h"
#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "cache/utils/local_filesystem.h"
#include "cache/utils/utils.h"

namespace dingofs {
namespace cache {
namespace utils {

using dingofs::base::file::StrMode;

template <typename... Args>
Status Posix::PosixError(int code, const char* format, const Args&... args) {
  // code
  auto status = Status::IoError("io error");
  switch (code) {
    case 0:
      status = Status::OK();
      break;
    case EINVAL:
      status = Status::InvalidParam("invalid param");
      break;
    case ENOENT:
      status = Status::NotFound("not found");
      break;
    case EEXIST:
      status = Status::Exist("exists");
      break;
    default:  // IO error
      break;
  }

  // log & update disk state
  std::string message = Errorf(code, format, args...);
  if (status.IsIoError() || status.IsInvalidParam()) {
    LOG(ERROR) << message;
  } else if (status.IsNotFound()) {
    LOG(WARNING) << message;
  }
  return status;
}

Status Posix::Stat(const std::string& path, struct stat* stat) {
  if (::stat(path.c_str(), stat) < 0) {
    return PosixError(errno, "stat(%s)", path);
  }
  return Status::OK();
}

Status Posix::MkDir(const std::string& path, uint16_t mode) {
  if (::mkdir(path.c_str(), mode) != 0) {
    return PosixError(errno, "mkdir(%s,%s)", path, StrMode(mode));
  }
  return Status::OK();
}

Status Posix::OpenDir(const std::string& path, ::DIR** dir) {
  *dir = ::opendir(path.c_str());
  if (nullptr == *dir) {
    return PosixError(errno, "opendir(%s)", path);
  }
  return Status::OK();
}

Status Posix::ReadDir(::DIR* dir, struct dirent** dirent) {
  errno = 0;
  *dirent = ::readdir(dir);
  if (nullptr == *dirent) {
    if (errno == 0) {  // no more files
      return Status::EndOfFile("end of file");
    }
    return PosixError(errno, "readdir()");
  }
  return Status::OK();
}

Status Posix::CloseDir(::DIR* dir) {
  ::closedir(dir);
  return Status::OK();
}

Status Posix::Create(const std::string& path, int* fd, bool use_direct) {
  int flags = O_TRUNC | O_WRONLY | O_CREAT;
  if (use_direct) {
    flags = flags | O_DIRECT;
  }
  *fd = ::open(path.c_str(), flags, 0644);
  if (*fd < 0) {
    return PosixError(errno, "open(%s,%#x,0644)", path, flags);
  }
  return Status::OK();
}

Status Posix::Open(const std::string& path, int flags, int* fd) {
  *fd = ::open(path.c_str(), flags);
  if (*fd < 0) {
    return PosixError(errno, "open(%s,%#x)", path, flags);
  }
  return Status::OK();
}

Status Posix::LSeek(int fd, off_t offset, int whence) {
  if (::lseek(fd, offset, whence) < 0) {
    return PosixError(errno, "lseek(%d,%d,%d)", fd, offset, whence);
  }
  return Status::OK();
}

Status Posix::Write(int fd, const char* buffer, size_t length) {
  while (length > 0) {
    ssize_t nwritten = ::write(fd, buffer, length);
    if (nwritten < 0) {
      if (errno == EINTR) {
        continue;  // retry
      }
      // error
      return PosixError(errno, "write(%d,%d)", fd, length);
    }
    // success
    buffer += nwritten;
    length -= nwritten;
  }

  return Status::OK();
}

Status Posix::Read(int fd, char* buffer, size_t length) {
  for (;;) {
    ssize_t n = ::read(fd, buffer, length);
    if (n < 0) {
      if (errno == EINTR) {
        continue;  // retry
      }
      // error
      return PosixError(errno, "read(%d,%d)", fd, length);
    }
    break;  // success
  }
  return Status::OK();
}

Status Posix::Close(int fd) {
  ::close(fd);
  return Status::OK();
}

Status Posix::Unlink(const std::string& path) {
  if (::unlink(path.c_str()) < 0) {
    return PosixError(errno, "unlink(%s)", path);
  }
  return Status::OK();
}

Status Posix::Link(const std::string& oldpath, const std::string& newpath) {
  if (::link(oldpath.c_str(), newpath.c_str()) < 0) {
    return PosixError(errno, "link(%s,%s)", oldpath, newpath);
  }
  return Status::OK();
}

Status Posix::Rename(const std::string& oldpath, const std::string& newpath) {
  if (::rename(oldpath.c_str(), newpath.c_str()) < 0) {
    return PosixError(errno, "rename(%s,%s)", oldpath, newpath);
  }
  return Status::OK();
}

Status Posix::StatFS(const std::string& path, struct statfs* statfs) {
  if (::statfs(path.c_str(), statfs) < 0) {
    return PosixError(errno, "statfs(%s)", path);
  }
  return Status::OK();
}

Status Posix::PosixFAdvise(int fd, off_t offset, size_t length, int advise) {
  if (::posix_fadvise(fd, offset, length, advise) != 0) {
    return PosixError(errno, "posix_fadvise(%d, 0, 0, %d)", fd, advise);
  }
  return Status::OK();
}

Status Posix::MMap(void* addr, size_t length, int port, int flags, int fd,
                   off_t offset, void** addr_out) {
  *addr_out = ::mmap(addr, length, port, flags, fd, offset);
  if (*addr_out == (void*)MAP_FAILED) {
    return PosixError(errno, "mmap(%p,%d,%d,%d,%d,%d)", addr, length, port,
                      flags, fd, offset);
  }
  return Status::OK();
}

Status Posix::MUnmap(void* addr, size_t length) {
  if (::munmap(addr, length) != 0) {
    return PosixError(errno, "munmap(%p,%d)", addr, length);
  }
  return Status::OK();
}

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
