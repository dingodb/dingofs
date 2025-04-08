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

#include "client/blockcache/local_filesystem.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/vfs.h>

#include <memory>

#include "absl/cleanup/cleanup.h"
#include "base/file/file.h"
#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "client/blockcache/helper.h"

namespace dingofs {
namespace client {
namespace blockcache {

using base::file::IsDir;
using base::file::IsFile;
using base::file::StrMode;
using base::filepath::ParentDir;
using base::filepath::PathJoin;
using base::math::Divide;
using base::math::kMiB;

// posix filesystem
PosixFileSystem::PosixFileSystem(
    std::shared_ptr<DiskStateMachine> disk_state_machine)
    : disk_state_machine_(disk_state_machine) {}

template <typename... Args>
Status PosixFileSystem::PosixError(int code, const char* format,
                                   const Args&... args) {
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
  std::string message = Helper::Errorf(code, format, args...);
  if (status.IsIoError() || status.IsInvalidParam()) {
    LOG(ERROR) << message;
  } else if (status.IsNotFound()) {
    LOG(WARNING) << message;
  }

  CheckError(status);
  return status;
}

void PosixFileSystem::CheckError(Status status) {
  if (disk_state_machine_ == nullptr) {
    return;
  }
  if (status.IsIoError()) {
    disk_state_machine_->IOErr();
  } else {
    disk_state_machine_->IOSucc();
  }
}

Status PosixFileSystem::Stat(const std::string& path, struct stat* stat) {
  if (::stat(path.c_str(), stat) < 0) {
    return PosixError(errno, "stat(%s)", path);
  }
  return Status::OK();
}

Status PosixFileSystem::MkDir(const std::string& path, uint16_t mode) {
  if (::mkdir(path.c_str(), mode) != 0) {
    return PosixError(errno, "mkdir(%s,%s)", path, StrMode(mode));
  }
  return Status::OK();
}

Status PosixFileSystem::OpenDir(const std::string& path, ::DIR** dir) {
  *dir = ::opendir(path.c_str());
  if (nullptr == *dir) {
    return PosixError(errno, "opendir(%s)", path);
  }
  return Status::OK();
}

Status PosixFileSystem::ReadDir(::DIR* dir, struct dirent** dirent) {
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

Status PosixFileSystem::CloseDir(::DIR* dir) {
  ::closedir(dir);
  return Status::OK();
}

Status PosixFileSystem::Create(const std::string& path, int* fd,
                               bool use_direct) {
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

Status PosixFileSystem::Open(const std::string& path, int flags, int* fd) {
  *fd = ::open(path.c_str(), flags);
  if (*fd < 0) {
    return PosixError(errno, "open(%s,%#x)", path, flags);
  }
  return Status::OK();
}

Status PosixFileSystem::LSeek(int fd, off_t offset, int whence) {
  if (::lseek(fd, offset, whence) < 0) {
    return PosixError(errno, "lseek(%d,%d,%d)", fd, offset, whence);
  }
  return Status::OK();
}

Status PosixFileSystem::Write(int fd, const char* buffer, size_t length) {
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

Status PosixFileSystem::Read(int fd, char* buffer, size_t length) {
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

Status PosixFileSystem::Close(int fd) {
  ::close(fd);
  return Status::OK();
}

Status PosixFileSystem::Unlink(const std::string& path) {
  if (::unlink(path.c_str()) < 0) {
    return PosixError(errno, "unlink(%s)", path);
  }
  return Status::OK();
}

Status PosixFileSystem::Link(const std::string& oldpath,
                             const std::string& newpath) {
  if (::link(oldpath.c_str(), newpath.c_str()) < 0) {
    return PosixError(errno, "link(%s,%s)", oldpath, newpath);
  }
  return Status::OK();
}

Status PosixFileSystem::Rename(const std::string& oldpath,
                               const std::string& newpath) {
  if (::rename(oldpath.c_str(), newpath.c_str()) < 0) {
    return PosixError(errno, "rename(%s,%s)", oldpath, newpath);
  }
  return Status::OK();
}

Status PosixFileSystem::StatFS(const std::string& path, struct statfs* statfs) {
  if (::statfs(path.c_str(), statfs) < 0) {
    return PosixError(errno, "statfs(%s)", path);
  }
  return Status::OK();
}

Status PosixFileSystem::FAdvise(int fd, int advise) {
  if (::posix_fadvise(fd, 0, 0, advise) != 0) {
    return PosixError(errno, "posix_fadvise(%d, 0, 0, %d)", fd, advise);
  }
  return Status::OK();
}

LocalFileSystem::LocalFileSystem(
    std::shared_ptr<DiskStateMachine> disk_state_machine)
    : posix_(std::make_shared<PosixFileSystem>(disk_state_machine)) {}

Status LocalFileSystem::MkDirs(const std::string& path) {
  // The parent diectory already exists in most time
  auto status = posix_->MkDir(path, 0755);
  if (status.ok()) {
    return status;
  } else if (status.IsExist()) {
    struct stat stat;
    status = posix_->Stat(path, &stat);
    if (!status.ok()) {
      return status;
    } else if (!IsDir(&stat)) {
      return Status::NotDirectory("not a directory");
    }
    return Status::OK();
  } else if (status.IsNotFound()) {  // parent directory not exist
    status = MkDirs(ParentDir(path));
    if (status.ok()) {
      status = MkDirs(path);
    }
  }
  return status;
}

Status LocalFileSystem::Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir;
  auto status = posix_->OpenDir(prefix, &dir);
  if (!status.ok()) {
    return status;
  }

  struct dirent* dirent;
  struct stat stat;
  auto defer = ::absl::MakeCleanup([dir, this]() { posix_->CloseDir(dir); });
  for (;;) {
    status = posix_->ReadDir(dir, &dirent);
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
    status = posix_->Stat(path, &stat);
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
  return status;
}

Status LocalFileSystem::WriteFile(const std::string& path, const char* buffer,
                                  size_t length, bool use_direct) {
  auto status = MkDirs(ParentDir(path));
  if (!status.ok()) {
    return status;
  }

  int fd;
  std::string tmp = path + ".tmp";
  if (use_direct) {
    use_direct = IsAligned(length) &&
                 IsAligned(reinterpret_cast<std::uintptr_t>(buffer));
  }
  status = posix_->Create(tmp, &fd, use_direct);
  if (status.ok()) {
    status = posix_->Write(fd, buffer, length);
    posix_->Close(fd);
    if (status.ok()) {
      status = posix_->Rename(tmp, path);
    }
  }
  return status;
}

Status LocalFileSystem::ReadFile(const std::string& path,
                                 std::shared_ptr<char>& buffer, size_t* length,
                                 bool drop_page_cache) {
  struct stat stat;
  auto status = posix_->Stat(path, &stat);
  if (!status.ok()) {
    return status;
  } else if (!IsFile(&stat)) {
    return Status::NotFound("not found");
  }

  size_t size = stat.st_size;
  if (size > kMiB * 4) {
    LOG(ERROR) << "File is too large: path=" << path << ", size=" << size;
    return Status::FileTooLarge("file too large");
  }

  int fd;
  status = posix_->Open(path, O_RDONLY, &fd);
  if (!status.ok()) {
    return status;
  }

  *length = size;
  buffer = std::shared_ptr<char>(new char[size], std::default_delete<char[]>());
  status = posix_->Read(fd, buffer.get(), size);

  if (status.ok() && drop_page_cache) {
    posix_->FAdvise(fd, POSIX_FADV_DONTNEED);
  }
  posix_->Close(fd);

  return status;
}

Status LocalFileSystem::RemoveFile(const std::string& path) {
  return posix_->Unlink(path);
}

Status LocalFileSystem::HardLink(const std::string& oldpath,
                                 const std::string& newpath) {
  auto status = MkDirs(ParentDir(newpath));
  if (status.ok()) {
    status = posix_->Link(oldpath, newpath);
  }
  return status;
}

bool LocalFileSystem::FileExists(const std::string& path) {
  struct stat stat;
  auto status = posix_->Stat(path, &stat);
  return status.ok() && IsFile(&stat);
}

Status LocalFileSystem::GetDiskUsage(const std::string& path, StatDisk* stat) {
  struct statfs statfs;
  auto status = posix_->StatFS(path, &statfs);
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
  return status;
}

Status LocalFileSystem::Do(DoFunc func) { return func(posix_); }

bool LocalFileSystem::IsAligned(uint64_t n) {
  return n % IO_ALIGNED_BLOCK_SIZE == 0;
}

std::shared_ptr<LocalFileSystem> NewTempLocalFileSystem() {
  return std::make_shared<LocalFileSystem>();
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
