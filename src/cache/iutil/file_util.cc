/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-01-14
 * Author: Jingli Chen (Wine93)
 */

#include "cache/iutil/file_util.h"

#include <absl/strings/str_join.h>
#include <butil/file_util.h>
#include <butil/memory/scope_guard.h>
#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/statfs.h>

#include <cerrno>
#include <unordered_map>

#include "cache/iutil/math_util.h"
#include "common/const.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace iutil {

Status PosixError(int syscode) {
  if (syscode == 0) {
    return Status::OK();
  } else if (syscode == EINVAL) {
    return Status::InvalidParam("invalid param");
  } else if (syscode == ENOENT) {
    return Status::NotFound("not found");
  } else if (syscode == EEXIST) {
    return Status::Exist("already exists");
  }
  return Status::IoError("io error");
}

std::string StrMode(uint16_t mode) {
  static std::unordered_map<uint16_t, char> type2char = {
      {S_IFSOCK, 's'}, {S_IFLNK, 'l'}, {S_IFREG, '-'}, {S_IFBLK, 'b'},
      {S_IFDIR, 'd'},  {S_IFCHR, 'c'}, {S_IFIFO, 'f'}, {0, '?'},
  };

  std::string s("?rwxrwxrwx");
  s[0] = type2char[mode & (S_IFMT & 0xffff)];
  if (mode & S_ISUID) {
    s[3] = 's';
  }
  if (mode & S_ISGID) {
    s[6] = 's';
  }
  if (mode & S_ISVTX) {
    s[9] = 't';
  }

  for (auto i = 0; i < 9; i++) {
    if ((mode & (1 << i)) == 0) {
      if ((s[9 - i] == 's') || (s[9 - i] == 't')) {
        s[9 - i] &= 0xDF;
      } else {
        s[9 - i] = '-';
      }
    }
  }
  return s;
}

std::string ParentDir(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return "/";
  }

  std::string parent = path.substr(0, index);
  if (parent.empty()) {
    return "/";
  }
  return parent;
}

bool FileIsExist(const std::string& path) {
  struct stat stat;
  int rc = ::stat(path.c_str(), &stat);
  if (rc < 0) {
    PLOG(WARNING) << "Fail to stat `" << path << "'";
    return false;
  } else if (S_ISREG(stat.st_mode)) {
    return true;
  }
  return false;
}

Status MkDirs(const std::string& path) {
  int rc = ::mkdir(path.c_str(), 0755);
  if (rc == 0) {
    return Status::OK();
  } else if (errno == EEXIST) {
    struct stat stat;
    rc = ::stat(path.c_str(), &stat);
    if (rc < 0) {
      PLOG(ERROR) << "Fail to stat `" << path << "'";
      return PosixError(errno);
    } else if (!S_ISDIR(stat.st_mode)) {
      LOG(ERROR) << "`" << path << "' is not a directory";
      return Status::NotDirectory("not a directory");
    }
    return Status::OK();
  } else if (errno == ENOENT) {  // parent directory not exist
    auto status = MkDirs(ParentDir(path));
    if (status.ok()) {
      status = MkDirs(path);
    }
    return status;
  }

  PLOG(ERROR) << "Fail to mkdir `" << path << "'";
  return PosixError(errno);
}

Status Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir = ::opendir(prefix.c_str());
  if (dir == nullptr) {
    PLOG(ERROR) << "Fail to opendir `" << prefix << "'";
    return PosixError(errno);
  }

  BRPC_SCOPE_EXIT { ::closedir(dir); };

  struct dirent* dirent;
  struct stat stat;
  Status status;
  for (;;) {
    errno = 0;
    dirent = ::readdir(dir);
    if (nullptr == dirent) {
      if (errno == 0) {  // end of file
        return Status::OK();
      }
      return PosixError(errno);
    }

    std::string name(dirent->d_name);
    if (name == "." || name == "..") {
      continue;
    }

    std::string path(absl::StrJoin({prefix, name}, "/"));
    int rc = ::stat(path.c_str(), &stat);
    if (rc != 0) {
      PLOG(ERROR) << "Fail to stat `" << path << "'";
      return PosixError(errno);
    } else if (S_ISDIR(stat.st_mode)) {
      status = Walk(path, func);
    } else {  // file
      iutil::TimeSpec atime(stat.st_atime, 0);
      status = func(prefix, FileInfo{name, stat.st_nlink, stat.st_size, atime});
    }

    if (!status.ok()) {
      break;
    }
  }

  return status;
}

Status Link(const std::string& src, const std::string& dest) {
  auto status = MkDirs(ParentDir(dest));
  if (!status.ok()) {
    return status;
  }

  int rc = ::link(src.c_str(), dest.c_str());
  if (rc < 0) {
    PLOG(ERROR) << "Fail to link `" << src << "' to `" << dest << "'";
    return PosixError(errno);
  }
  return Status::OK();
}

Status Unlink(const std::string& path) {
  if (::unlink(path.c_str()) < 0) {
    PLOG(ERROR) << "Fail to unlink `" << path << "'";
    return PosixError(errno);
  }
  return Status::OK();
}

Status Rename(const std::string& oldpath, const std::string& newpath) {
  if (::rename(oldpath.c_str(), newpath.c_str()) < 0) {
    PLOG(ERROR) << "Fail to rename `" << oldpath << "' to `" << newpath << "'";
    return PosixError(errno);
  }
  return Status::OK();
}

Status Stat(const std::string& path, FileInfo* info) {
  struct stat stat;
  if (::stat(path.c_str(), &stat) < 0) {
    PLOG(ERROR) << "Fail to stat `" << path << "'";
    return PosixError(errno);
  }

  info->name = path;
  info->nlink = stat.st_nlink;
  info->size = stat.st_size;
  info->atime = iutil::TimeSpec(stat.st_atime, 0);
  return Status::OK();
}

Status StatFS(const std::string& path, struct StatFS* stat) {
  struct statfs statfs;
  if (::statfs(path.c_str(), &statfs) < 0) {
    PLOG(ERROR) << "Fail to statfs `" << path << "'";
    return PosixError(errno);
  }

  stat->total_bytes = statfs.f_blocks * statfs.f_bsize;
  stat->total_files = statfs.f_files;
  stat->free_bytes = statfs.f_bfree * statfs.f_bsize;
  stat->free_files = statfs.f_ffree;
  stat->free_bytes_ratio = double(stat->free_bytes) / stat->total_bytes;
  stat->free_files_ratio = double(stat->free_files) / stat->total_files;
  return Status::OK();
}

Status CreateFile(const std::string& filepath, int mode, int* fd) {
  *fd = ::creat(filepath.c_str(), mode);
  if (*fd < 0) {
    PLOG(ERROR) << "Fail to create file `" << filepath << "'";
    return PosixError(errno);
  }
  return Status::OK();
}

Status OpenFile(const std::string& filepath, int flags, int* fd) {
  *fd = ::open(filepath.c_str(), flags);
  if (*fd < 0) {
    PLOG(ERROR) << "Fail to open file `" << filepath << "'";
    return PosixError(errno);
  }
  return Status::OK();
}

Status OpenFile(const std::string& filepath, int flags, int mode, int* fd) {
  *fd = ::open(filepath.c_str(), flags, mode);
  if (*fd < 0) {
    PLOG(ERROR) << "Fail to open file `" << filepath << "'";
    return PosixError(errno);
  }
  return Status::OK();
}

Status WriteFile(const std::string& filepath, const std::string& content) {
  auto status = MkDirs(ParentDir(filepath));
  if (!status.ok()) {
    LOG(ERROR) << "Fail to mkdirs `" << ParentDir(filepath) << "'";
    return status;
  }

  int rc = butil::WriteFile(butil::FilePath(filepath), content.data(),
                            content.size());
  if (rc == static_cast<int>(content.size())) {
    return Status::OK();
  }

  PLOG(ERROR) << "Fail to write content to file=`" << filepath << "'";
  return Status::IoError("write file failed");
}

Status ReadFile(const std::string& filepath, std::string* content) {
  if (!FileIsExist(filepath)) {
    return Status::NotFound("file not found");
  } else if (butil::ReadFileToString(butil::FilePath(filepath), content),
             4 * kMiB) {
    return Status::OK();
  }

  LOG(ERROR) << "Fail to read content from file=`" << filepath << "'";
  return Status::IoError("read file failed");
}

Status Close(int fd) {
  if (::close(fd) < 0) {
    PLOG(ERROR) << "Fail to close fd=" << fd;
    return PosixError(errno);
  }
  return Status::OK();
}

Status Fallocate(int fd, int mode, off_t offset, size_t len) {
  if (::fallocate(fd, mode, offset, len) < 0) {
    PLOG(ERROR) << "Fail to fallocate file";
    return PosixError(errno);
  }
  return Status::OK();
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
