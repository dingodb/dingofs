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

#include "common/blockaccess/files/file_accesser.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <thread>

#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace blockaccess {

namespace {

namespace fs = std::filesystem;

Status PosixError(const std::string context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(error_number, context, std::strerror(error_number));
  } else {
    return Status::IoError(error_number, context, std::to_string(error_number));
  }
}

Status CreateDir(const std::string& dirname) {
  if (::mkdir(dirname.c_str(), 0755) != 0) {
    return PosixError(dirname, errno);
  }
  return Status::OK();
}

Status RemoveDir(const std::string& dirname) {
  if (::rmdir(dirname.c_str()) != 0) {
    return PosixError(dirname, errno);
  }
  return Status::OK();
}

bool FileExistes(const std::string& filename) {
  return ::access(filename.c_str(), F_OK) == 0;
}

Status FileSize(const std::string& filename, uint64_t* size) {
  struct ::stat file_stat;
  if (::stat(filename.c_str(), &file_stat) != 0) {
    *size = 0;
    return PosixError(filename, errno);
  }
  *size = file_stat.st_size;
  return Status::OK();
}

Status RenameFile(const std::string& from, const std::string& to) {
  if (::rename(from.c_str(), to.c_str()) != 0) {
    return PosixError(from + " to " + to, errno);
  }
  return Status::OK();
}

Status RemoveFile(const std::string& filename) {
  if (::remove(filename.c_str()) != 0) {
    return PosixError(filename, errno);
  }
  return Status::OK();
}

// extract open/close into file related class
Status Open(const std::string& filename, int flags, mode_t mode, int* fd) {
  int _fd = ::open(filename.c_str(), flags, mode);
  if (_fd < 0) {
    return PosixError(filename, errno);
  }

  *fd = _fd;
  return Status::OK();
}

Status Open(const std::string& filename, int flags, int* fd) {
  int _fd = ::open(filename.c_str(), flags);
  if (_fd < 0) {
    return PosixError(filename, errno);
  }

  *fd = _fd;
  return Status::OK();
}

Status Close(const std::string& filename, int fd) {
  if (::close(fd) != 0) {
    return PosixError(filename, errno);
  }
  return Status::OK();
}

}  // namespace

bool FileAccesser::Init() {
  if (started_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "FileAccesser already started";
    return false;
  }

  if (root_.empty()) {
    LOG(ERROR) << "FileAccesser root is empty";
    return false;
  }

  LOG(INFO) << "FileAccesser root path: " << root_;

  // recursive create container path if not eixts
  auto container_path = fs::path(root_);
  if (!fs::exists(container_path)) {
    fs::create_directories(container_path);
  }

  started_.store(true, std::memory_order_relaxed);
  return true;
}

bool FileAccesser::Destroy() {
  if (!started_.load(std::memory_order_relaxed)) {
    return true;
  }

  started_.store(false, std::memory_order_relaxed);
  return true;
}

bool FileAccesser::ContainerExist() { return FileExistes(root_); }

std::string FileAccesser::KeyPath(const std::string& key) {
  if (root_.back() == '/') {
    return (fs::path(root_) / key).lexically_normal().string();
  } else {
    return fs::path(root_ + "/" + key).lexically_normal().string();
  }
}

Status FileAccesser::Put(const std::string& key, const char* buffer,
                         size_t length) {
  CHECK_GT(length, 0);
  std::string fpath = KeyPath(key);
  std::string tmp = fpath + ".tmp";

  // recursive create parent path if not eixts
  auto parent_path = fs::path(tmp).parent_path();
  if (!fs::exists(parent_path)) {
    fs::create_directories(parent_path);
  }

  {
    int fd = -1;
    DINGOFS_RETURN_NOT_OK(Open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644, &fd));

    SCOPED_CLEANUP({ Close(tmp, fd); });
    // TOOO: remove tmp file on error

    while (length > 0) {
      ssize_t res = ::write(fd, buffer, length);
      if (res < 0) {
        if (errno == EINTR) {
          continue;
        }
        return PosixError(fpath, errno);
      }
      buffer += res;
      length -= res;
    }
  }

  DINGOFS_RETURN_NOT_OK(RenameFile(tmp, fpath));
  return Status::OK();
}

void FileAccesser::DoAsyncPut(PutObjectAsyncContextSPtr context) {
  context->status = Put(context->key, context->buffer, context->buffer_size);
  context->cb(context);
}

void FileAccesser::AsyncPut(PutObjectAsyncContextSPtr context) {
  std::thread([&, context]() { DoAsyncPut(context); }).detach();
}

Status FileAccesser::Get(const std::string& key, std::string* data) {
  std::string fpath = KeyPath(key);

  int fd = -1;
  DINGOFS_RETURN_NOT_OK(Open(fpath, O_RDONLY, &fd));

  SCOPED_CLEANUP({ Close(fpath, fd); });

  uint64_t size = 0;
  DINGOFS_RETURN_NOT_OK(FileSize(fpath, &size));

  if (size == 0) {
    data->clear();
    return Status::OK();
  }

  data->resize(size);
  char* buffer = data->data();

  {
    while (true) {
      ssize_t res = ::read(fd, buffer, size);
      if (res < 0) {
        if (errno == EINTR) {
          continue;
        }
        return PosixError(fpath, errno);
      }
      break;
    }
  }

  return Status::OK();
}

Status FileAccesser::RangeRead(const std::string& key, off_t offset,
                               size_t length, char* buffer,
                               size_t* readed_size) {
  std::string fpath = KeyPath(key);

  uint64_t size = 0;
  DINGOFS_RETURN_NOT_OK(FileSize(fpath, &size));

  if (offset < 0 || static_cast<uint64_t>(offset) > size) {
    return Status::EndOfFile(key, "Read range out of file size");
  }

  int fd = -1;
  DINGOFS_RETURN_NOT_OK(Open(fpath, O_RDONLY, &fd));

  SCOPED_CLEANUP({ Close(fpath, fd); });

  size_t total_read = 0;
  while (total_read < length) {
    ssize_t read_size = ::pread(fd, buffer + total_read, length - total_read,
                                offset + total_read);
    if (read_size < 0) {
      return PosixError(fpath, errno);
    }

    if (read_size == 0) {
      break;  // EOF reached
    }
    total_read += read_size;
  }

  if (readed_size) {
    *readed_size = total_read;
  }

  return Status::OK();
}

Status FileAccesser::Range(const std::string& key, off_t offset, size_t length,
                           char* buffer) {
  size_t total_read = 0;
  DINGOFS_RETURN_NOT_OK(RangeRead(key, offset, length, buffer, &total_read));

  if (total_read != length) {
    return Status::EndOfFile(key, "Could not read enough data");
  }
  return Status::OK();
}

void FileAccesser::DoAsyncGet(GetObjectAsyncContextSPtr context) {
  size_t total_read = 0;
  context->status = RangeRead(context->key, context->offset, context->len,
                              context->buf, &total_read);
  context->actual_len = total_read;
  context->cb(context);
}

void FileAccesser::AsyncGet(GetObjectAsyncContextSPtr context) {
  std::thread([&, context]() { DoAsyncGet(context); }).detach();
}

bool FileAccesser::BlockExist(const std::string& key) {
  std::string fpath = KeyPath(key);
  return FileExistes(fpath);
}

Status FileAccesser::Delete(const std::string& key) {
  std::string fpath = KeyPath(key);
  Status s = RemoveFile(fpath);
  if (s.IsNotFound()) {
    return Status::OK();
  }
  return s;
}

Status FileAccesser::BatchDelete(const std::list<std::string>& keys) {
  for (const auto& key : keys) {
    Status s = Delete(key);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

}  // namespace blockaccess
}  // namespace dingofs