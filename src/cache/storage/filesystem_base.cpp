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

#include "cache/storage/filesystem_base.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/vfs.h>

#include <cstddef>
#include <memory>

#include "absl/cleanup/cleanup.h"
#include "base/file/file.h"
#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "base/time/time.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/linux_io_uring.h"
#include "cache/storage/filesystem.h"
#include "cache/utils/filepath.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"

namespace dingofs {
namespace cache {

using dingofs::base::file::IsDir;
using dingofs::base::file::IsFile;
using dingofs::base::math::Divide;
using dingofs::base::time::TimeSpec;

FileSystemBase::FileSystemBase(CheckStatusFunc check_status_func)
    : check_status_func_(check_status_func) {}

FileSystemBase& FileSystemBase::GetInstance() {
  static FileSystemBase instance;
  return instance;
}

Status FileSystemBase::Init() { return Status::OK(); }

Status FileSystemBase::Destroy() { return Status::OK(); }

Status FileSystemBase::MkDirs(const std::string& path) {
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
    status = MkDirs(FilePath::ParentDir(path));
    if (status.ok()) {
      status = MkDirs(path);
    }
  }
  return CheckStatus(status);
}

Status FileSystemBase::Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir;
  auto status = Posix::OpenDir(prefix, &dir);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  struct dirent* dirent;
  struct stat stat;
  auto defer = absl::MakeCleanup([dir]() { Posix::CloseDir(dir); });
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

    std::string path(FilePath::PathJoin({prefix, name}));
    status = Posix::Stat(path, &stat);
    if (!status.ok()) {
      // break
    } else if (IsDir(&stat)) {
      status = Walk(path, func);
    } else {  // file
      TimeSpec atime(stat.st_atime, 0);
      status = func(prefix, FileInfo(name, stat.st_nlink, stat.st_size, atime));
    }

    if (!status.ok()) {
      break;
    }
  }
  return CheckStatus(status);
}

Status FileSystemBase::WriteFile(const std::string& /*path*/,
                                 const IOBuffer& /* buffer*/,
                                 WriteOption /*option*/) {
  return Status::NotSupport("filesystem base does not support write");
}

Status FileSystemBase::ReadFile(const std::string& /*path*/, off_t /*offset*/,
                                size_t /*length*/, IOBuffer* /*buffer*/,
                                ReadOption /*option*/) {
  return Status::NotSupport("filesystem base does not support read");
};

Status FileSystemBase::RemoveFile(const std::string& path) {
  return CheckStatus(Posix::Unlink(path));
}

Status FileSystemBase::Link(const std::string& from, const std::string& to) {
  auto status = MkDirs(FilePath::ParentDir(to));
  if (status.ok()) {
    status = Posix::Link(from, to);
  }
  return CheckStatus(status);
}

bool FileSystemBase::FileExists(const std::string& path) {
  struct stat stat;
  auto status = Posix::Stat(path, &stat);
  return status.ok() && IsFile(&stat);
}

Status FileSystemBase::StatFile(const std::string& path, FileInfo* info) {
  struct stat stat;
  auto status = Posix::Stat(path, &stat);
  if (status.ok()) {
    info->name = path;
    info->nlink = stat.st_nlink;
    info->size = stat.st_size;
    info->atime = TimeSpec(stat.st_atime, 0);
  }
  return CheckStatus(status);
}

Status FileSystemBase::StatFS(const std::string& path, FSStat* stat) {
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

Status FileSystemBase::CheckStatus(Status status) {
  // if (check_status_func_ != nullptr) {
  //   return check_status_func_(status);
  // }
  return status;
}

}  // namespace cache
}  // namespace dingofs
