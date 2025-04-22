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

#include "absl/cleanup/cleanup.h"
#include "base/file/file.h"
#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "cache/common/posix.h"
#include "cache/common/sys_conf.h"
#include "cache/common/utils.h"

namespace dingofs {
namespace cache {
namespace common {

using base::file::IsDir;
using base::file::IsFile;
using base::filepath::ParentDir;
using base::filepath::PathJoin;
using base::math::Divide;
using base::math::kMiB;

LocalFileSystem::LocalFileSystem() : check_status_func_(nullptr) {}

LocalFileSystem::LocalFileSystem(CheckStatusFunc check_status_func)
    : check_status_func_(check_status_func) {}

Status LocalFileSystem::CheckStatus(Status status) {
  if (check_status_func_ != nullptr) {
    return check_status_func_(status);
  }
  return status;
}

bool LocalFileSystem::IsAligned(uint64_t n) {
  return n % (SysConf::GetAlignedBlockSize()) == 0;
}

Status LocalFileSystem::MkDirs(const std::string& path) {
  // The parent diectory already exists in most time
  auto status = Posix::MkDir(path, 0755);
  if (status.ok()) {
    return CheckStatus(status);
  } else if (status.IsExist()) {
    struct stat stat;
    status = Posix::Stat(path, &stat);
    if (!status.ok()) {
      return CheckStatus(status);
    } else if (!IsDir(&stat)) {
      return CheckStatus(Status::NotDirectory("not a directory"));
    }
    return CheckStatus(Status::OK());
  } else if (status.IsNotFound()) {  // parent directory not exist
    status = MkDirs(ParentDir(path));
    if (status.ok()) {
      status = MkDirs(path);
    }
  }
  return CheckStatus(status);
}

Status LocalFileSystem::Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir;
  auto status = Posix::OpenDir(prefix, &dir);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  struct dirent* dirent;
  struct stat stat;
  auto defer = absl::MakeCleanup([dir, this]() { Posix::CloseDir(dir); });
  for (;;) {
    status = Posix::ReadDir(dir, &dirent);
    if (status.IsEndOfFile()) {
      status = Status::OK();
      break;
    } else if (!status.ok()) {
      break;
    }

    std::string name(dirent->d_name);
    if (name == "." || name == "..") {
      continue;
    }

    std::string path(PathJoin({prefix, name}));
    status = Posix::Stat(path, &stat);
    if (!status.ok()) {
      // break
    } else if (IsDir(&stat)) {
      status = Walk(path, func);
    } else {  // file
      TimeSpec atime(stat.st_atime, 0);
      status = func(prefix, FileInfo(name, stat.st_size, atime));
    }

    if (!status.ok()) {
      break;
    }
  }
  return CheckStatus(status);
}

Status LocalFileSystem::WriteFile(const std::string& path, const char* buffer,
                                  size_t length, bool use_direct) {
  auto status = MkDirs(ParentDir(path));
  if (!status.ok()) {
    return CheckStatus(status);
  }

  int fd;
  std::string tmp = path + ".tmp";
  if (use_direct) {
    use_direct = IsAligned(length) &&
                 IsAligned(reinterpret_cast<std::uintptr_t>(buffer));
  }
  status = Posix::Create(tmp, &fd, use_direct);
  if (status.ok()) {
    status = Posix::Write(fd, buffer, length);
    Posix::Close(fd);
    if (status.ok()) {
      status = Posix::Rename(tmp, path);
    }
  }
  return CheckStatus(status);
}

Status LocalFileSystem::ReadFile(const std::string& path,
                                 std::shared_ptr<char>& buffer, size_t* length,
                                 bool drop_page_cache) {
  struct stat stat;
  auto status = Posix::Stat(path, &stat);
  if (!status.ok()) {
    return CheckStatus(status);
  } else if (!IsFile(&stat)) {
    return CheckStatus(Status::NotFound("not found"));
  }

  size_t size = stat.st_size;
  if (size > kMiB * 4) {
    LOG(ERROR) << "File is too large: path=" << path << ", size=" << size;
    return CheckStatus(Status::FileTooLarge("file too large"));
  }

  int fd;
  status = Posix::Open(path, O_RDONLY, &fd);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  *length = size;
  buffer = std::shared_ptr<char>(new char[size], std::default_delete<char[]>());
  status = Posix::Read(fd, buffer.get(), size);

  if (status.ok() && drop_page_cache) {
    Posix::PosixFAdvise(fd, POSIX_FADV_DONTNEED);
  }
  Posix::Close(fd);

  return CheckStatus(status);
}

Status LocalFileSystem::RemoveFile(const std::string& path) {
  return CheckStatus(Posix::Unlink(path));
}

Status LocalFileSystem::HardLink(const std::string& oldpath,
                                 const std::string& newpath) {
  auto status = MkDirs(ParentDir(newpath));
  if (status.ok()) {
    status = Posix::Link(oldpath, newpath);
  }
  return CheckStatus(status);
}

bool LocalFileSystem::FileExists(const std::string& path) {
  struct stat stat;
  auto status = Posix::Stat(path, &stat);
  return status.ok() && IsFile(&stat);
}

Status LocalFileSystem::GetDiskUsage(const std::string& path, StatDisk* stat) {
  struct statfs statfs;
  auto status = Posix::StatFS(path, &statfs);
  if (status.ok()) {
    stat->total_bytes = statfs.f_blocks * statfs.f_bsize;
    stat->total_files = statfs.f_files;
    stat->free_bytes = statfs.f_bfree * statfs.f_bsize;
    stat->free_files = statfs.f_ffree;
    stat->free_bytes_ratio = 1;
    stat->free_files_ratio = 1;
    stat->free_bytes_ratio = Divide(stat->free_bytes, stat->total_bytes);
    stat->free_files_ratio = Divide(stat->free_files, stat->total_files);
  }
  return CheckStatus(status);
}

Status LocalFileSystem::Do(DoFunc func) { return CheckStatus(func()); }

}  // namespace common
}  // namespace cache
}  // namespace dingofs
