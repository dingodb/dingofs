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

#include "cache/common/local_filesystem.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/vfs.h>

#include <memory>
#include <sstream>

#include "absl/cleanup/cleanup.h"
#include "base/file/file.h"
#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "base/string/string.h"
#include "cache/common/errno.h"
#include "cache/common/posix.h"
#include "client/common/dynamic_config.h"

namespace dingofs {
namespace cache {
namespace common {

using base::file::IsDir;
using base::file::IsFile;
using base::file::StrMode;
using base::filepath::Filename;
using base::filepath::ParentDir;
using base::filepath::PathJoin;
using base::math::Divide;
using base::math::kMiB;
using base::string::StrFormat;
using cache::common::Posix;

LocalFileSystem::LocalFileSystem(CheckErrFunc check_err_func)
    : check_err_func_(check_err_func) {}

Errno LocalFileSystem::CheckErr(Errno rc) {
  if (check_err_func_ != nullptr) {
    return check_err_func_(rc);
  }
  return rc;
}

Errno LocalFileSystem::MkDirs(const std::string& path) {
  // The parent diectory already exists in most time
  auto rc = Posix::MkDir(path, 0755);
  if (rc == Errno::OK) {
    return CheckErr(rc);
  } else if (rc == Errno::EXISTS) {
    struct stat stat;
    rc = Posix::Stat(path, &stat);
    if (rc != Errno::OK) {
      return CheckErr(rc);
    } else if (!IsDir(&stat)) {
      return CheckErr(Errno::NOT_DIRECTORY);
    }
    return CheckErr(Errno::OK);
  } else if (rc == Errno::NOT_FOUND) {  // parent directory not exist
    rc = MkDirs(ParentDir(path));
    if (rc == Errno::OK) {
      rc = MkDirs(path);
    }
  }
  return CheckErr(rc);
}

Errno LocalFileSystem::Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir;
  auto rc = Posix::OpenDir(prefix, &dir);
  if (rc != Errno::OK) {
    return CheckErr(rc);
  }

  struct dirent* dirent;
  struct stat stat;
  auto defer = absl::MakeCleanup([dir, this]() { Posix::CloseDir(dir); });
  for (;;) {
    rc = Posix::ReadDir(dir, &dirent);
    if (rc == Errno::END_OF_FILE) {
      rc = Errno::OK;
      break;
    } else if (rc != Errno::OK) {
      break;
    }

    std::string name(dirent->d_name);
    if (name == "." || name == "..") {
      continue;
    }

    std::string path(PathJoin({prefix, name}));
    rc = Posix::Stat(path, &stat);
    if (rc != Errno::OK) {
      // break
    } else if (IsDir(&stat)) {
      rc = Walk(path, func);
    } else {  // file
      TimeSpec atime(stat.st_atime, 0);
      rc = func(prefix, FileInfo(name, stat.st_size, atime));
    }

    if (rc != Errno::OK) {
      break;
    }
  }
  return CheckErr(rc);
}

Errno LocalFileSystem::WriteFile(const std::string& path, const char* buffer,
                                 size_t length, bool use_direct) {
  auto rc = MkDirs(ParentDir(path));
  if (rc != Errno::OK) {
    return CheckErr(rc);
  }

  int fd;
  std::string tmp = path + ".tmp";
  if (use_direct) {
    use_direct = IsAligned(length) &&
                 IsAligned(reinterpret_cast<std::uintptr_t>(buffer));
  }
  rc = Posix::Create(tmp, &fd, use_direct);
  if (rc == Errno::OK) {
    rc = Posix::Write(fd, buffer, length);
    Posix::Close(fd);
    if (rc == Errno::OK) {
      rc = Posix::Rename(tmp, path);
    }
  }
  return CheckErr(rc);
}

Errno LocalFileSystem::ReadFile(const std::string& path,
                                std::shared_ptr<char>& buffer,
                                size_t* length, ) {
  struct stat stat;
  auto rc = Posix::Stat(path, &stat);
  if (rc != Errno::OK) {
    return CheckErr(rc);
  } else if (!IsFile(&stat)) {
    return CheckErr(Errno::NOT_FOUND);
  }

  size_t size = stat.st_size;
  if (size > kMiB * 4) {
    LOG(ERROR) << "File is too large: path=" << path << ", size=" << size;
    return CheckErr(Errno::FILE_TOO_LARGE);
  }

  int fd;
  rc = Posix::Open(path, O_RDONLY, &fd);
  if (rc != Errno::OK) {
    return CheckErr(rc);
  }

  *length = size;
  buffer = std::shared_ptr<char>(new char[size], std::default_delete<char[]>());
  rc = Posix::Read(fd, buffer.get(), size);
  Posix::Close(fd);
  return CheckErr(rc);
}

Errno LocalFileSystem::RemoveFile(const std::string& path) {
  return CheckErr(Posix::Unlink(path));
}

Errno LocalFileSystem::HardLink(const std::string& oldpath,
                                const std::string& newpath) {
  auto rc = MkDirs(ParentDir(newpath));
  if (rc == Errno::OK) {
    rc = Posix::Link(oldpath, newpath);
  }
  return CheckErr(rc);
}

bool LocalFileSystem::FileExists(const std::string& path) {
  struct stat stat;
  auto rc = Posix::Stat(path, &stat);
  return rc == Errno::OK && IsFile(&stat);
}

Errno LocalFileSystem::GetDiskUsage(const std::string& path, StatDisk* stat) {
  struct statfs statfs;
  auto rc = Posix::StatFS(path, &statfs);
  if (rc == Errno::OK) {
    stat->total_bytes = statfs.f_blocks * statfs.f_bsize;
    stat->total_files = statfs.f_files;
    stat->free_bytes = statfs.f_bfree * statfs.f_bsize;
    stat->free_files = statfs.f_ffree;
    stat->free_bytes_ratio = Divide(stat->free_bytes, stat->total_bytes);
    stat->free_files_ratio = Divide(stat->free_files, stat->total_files);
  }
  return CheckErr(rc);
}

Errno LocalFileSystem::Do(DoFunc func) { return CheckErr(func()); }

bool LocalFileSystem::IsAligned(uint64_t n) {
  return n % IO_ALIGNED_BLOCK_SIZE == 0;
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
