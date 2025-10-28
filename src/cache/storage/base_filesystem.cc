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

#include "cache/storage/base_filesystem.h"

#include <gflags/gflags.h>

#include "cache/common/macro.h"
#include "cache/storage/filesystem.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"
#include "utils/time.h"

namespace dingofs {
namespace cache {

BaseFileSystem::BaseFileSystem(CheckStatusFunc check_status_func)
    : check_status_func_(check_status_func) {}

BaseFileSystem& BaseFileSystem::GetInstance() {
  static BaseFileSystem instance;
  return instance;
}

Status BaseFileSystem::Start() { return Status::OK(); }

Status BaseFileSystem::Shutdown() { return Status::OK(); }

Status BaseFileSystem::MkDirs(const std::string& path) {
  // The parent diectory already exists in most time
  auto status = Posix::MkDir(path, 0755);
  if (status.ok()) {
    return CheckStatus(status);
  } else if (status.IsExist()) {
    struct stat stat;
    status = Posix::Stat(path, &stat);
    if (!status.ok()) {
      return CheckStatus(status);
    } else if (!Helper::IsDir(&stat)) {
      return CheckStatus(Status::NotDirectory("not a directory"));
    }
    return CheckStatus(Status::OK());
  } else if (status.IsNotFound()) {  // parent directory not exist
    status = MkDirs(Helper::ParentDir(path));
    if (status.ok()) {
      status = MkDirs(path);
    }
  }
  return CheckStatus(status);
}

Status BaseFileSystem::Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir;
  auto status = Posix::OpenDir(prefix, &dir);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  struct dirent* dirent;
  struct stat stat;
  SCOPE_EXIT { Posix::CloseDir(dir); };
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

    std::string path(Helper::PathJoin({prefix, name}));
    status = Posix::Stat(path, &stat);
    if (!status.ok()) {
      // break
    } else if (Helper::IsDir(&stat)) {
      status = Walk(path, func);
    } else {  // file
      utils::TimeSpec atime(stat.st_atime, 0);
      status = func(prefix, FileInfo(name, stat.st_nlink, stat.st_size, atime));
    }

    if (!status.ok()) {
      break;
    }
  }
  return CheckStatus(status);
}

Status BaseFileSystem::WriteFile(ContextSPtr /*ctx*/,
                                 const std::string& /*path*/,
                                 const IOBuffer& /* buffer*/,
                                 WriteOption /*option*/) {
  return Status::NotSupport("filesystem base does not support write");
}

Status BaseFileSystem::ReadFile(ContextSPtr /*ctx*/,
                                const std::string& /*path*/, off_t /*offset*/,
                                size_t /*length*/, IOBuffer* /*buffer*/,
                                ReadOption /*option*/) {
  return Status::NotSupport("filesystem base does not support read");
};

Status BaseFileSystem::RemoveFile(const std::string& path) {
  return CheckStatus(Posix::Unlink(path));
}

Status BaseFileSystem::Link(const std::string& from, const std::string& to) {
  auto status = MkDirs(Helper::ParentDir(to));
  if (status.ok()) {
    status = Posix::Link(from, to);
  }
  return CheckStatus(status);
}

bool BaseFileSystem::FileExists(const std::string& path) {
  struct stat stat;
  auto status = Posix::Stat(path, &stat);
  return status.ok() && Helper::IsFile(&stat);
}

Status BaseFileSystem::StatFile(const std::string& path, FileInfo* info) {
  struct stat stat;
  auto status = Posix::Stat(path, &stat);
  if (status.ok()) {
    info->name = path;
    info->nlink = stat.st_nlink;
    info->size = stat.st_size;
    info->atime = utils::TimeSpec(stat.st_atime, 0);
  }
  return CheckStatus(status);
}

Status BaseFileSystem::StatFS(const std::string& path, FSStat* stat) {
  struct statfs statfs;
  auto status = Posix::StatFS(path, &statfs);
  if (status.ok()) {
    stat->total_bytes = statfs.f_blocks * statfs.f_bsize;
    stat->total_files = statfs.f_files;
    stat->free_bytes = statfs.f_bfree * statfs.f_bsize;
    stat->free_files = statfs.f_ffree;
    stat->free_bytes_ratio = 1;
    stat->free_files_ratio = 1;
    stat->free_bytes_ratio =
        Helper::Divide(stat->free_bytes, stat->total_bytes);
    stat->free_files_ratio =
        Helper::Divide(stat->free_files, stat->total_files);
  }
  return CheckStatus(status);
}

Status BaseFileSystem::CheckStatus(Status status) {
  if (check_status_func_ != nullptr) {
    return check_status_func_(status);
  }
  return status;
}

Status FSUtil::MkDirs(const std::string& dir) {
  return BaseFileSystem::GetInstance().MkDirs(dir);
}

Status FSUtil::Walk(const std::string& dir, WalkFunc walk_func) {
  return BaseFileSystem::GetInstance().Walk(dir, walk_func);
}

Status FSUtil::WriteFile(const std::string& filepath,
                         const std::string& content) {
  int rc = butil::WriteFile(butil::FilePath(filepath), content.data(),
                            content.size());
  if (rc == static_cast<int>(content.size())) {
    return Status::OK();
  }
  return Status::IoError("write file failed");
}

Status FSUtil::ReadFile(const std::string& filepath, std::string* content) {
  if (!FileExists(filepath)) {
    return Status::NotFound("file not found");
  } else if (butil::ReadFileToString(butil::FilePath(filepath), content),
             4 * kMiB) {
    return Status::OK();
  }
  return Status::IoError("read file failed");
}

Status FSUtil::RemoveFile(const std::string& filepath) {
  return BaseFileSystem::GetInstance().RemoveFile(filepath);
}

bool FSUtil::FileExists(const std::string& filepath) {
  return BaseFileSystem::GetInstance().FileExists(filepath);
}

Status FSUtil::StatFS(const std::string& dir, FSStat* stat) {
  return BaseFileSystem::GetInstance().StatFS(dir, stat);
}

}  // namespace cache
}  // namespace dingofs
