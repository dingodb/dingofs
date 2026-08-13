/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "cache/v2/store/local_filesystem.h"

#include <filesystem>
#include <string>
#include <utility>

#include "cache/v2/core/fs/filesystem.h"
#include "cache/v2/store/health.h"
#include "cache/v2/store/layout.h"
#include "cache/v2/utils/align.h"

namespace dingofs {
namespace cache {
namespace v2 {

Future<Status> LocalFileSystem::WriteFile(std::string path, BufferViews block) {
  const Status check = CheckBlock(block);
  if (!check.ok()) {
    co_return check;
  }

  // mkdir
  const Status mkdir = co_await FileSystem::MakeDirs(
      std::filesystem::path(path).parent_path().string());
  if (!mkdir.ok()) {
    co_return Report(mkdir);
  }

  // open
  const bool direct =
      block.size() == 1 && IsAligned(block[0].data, kBlockAlign);
  std::string tmp_path = DiskCacheLayout::TempPath(path);
  StatusOr<File> open = co_await OpenFile(
      tmp_path, OpenFlags::kWrite | OpenFlags::kCreate, direct);
  if (!open.ok()) {
    co_return Report(open.status());
  }

  // write
  File file = std::move(open).value();
  Status status = co_await WriteFile(&file, block, direct);
  const Status close = co_await file.Close();
  if (status.ok()) {
    status = close;
  }

  // rename
  if (status.ok()) {
    status = co_await FileSystem::Rename(std::move(tmp_path), std::move(path));
  } else {
    (void)co_await FileSystem::Unlink(std::move(tmp_path));
  }
  co_return Report(status);
}

Future<Status> LocalFileSystem::ReadFile(std::string path, uint64_t offset,
                                         uint32_t length, char* buffer) {
  const bool direct = IsAligned(buffer, kBlockAlign) &&
                      IsAligned(offset, kBlockAlign) &&
                      IsAligned(length, kBlockAlign);

  // open
  StatusOr<File> open =
      co_await OpenFile(std::move(path), OpenFlags::kRead, direct);
  if (!open.ok()) {
    co_return Report(open.status());
  }

  // read
  File file = std::move(open).value();
  Status status = Status::OK();
  StatusOr<size_t> nread = co_await file.Read(offset, buffer, length);
  if (!nread.ok()) {
    status = nread.status();
  } else if (*nread < length) {
    status = Status::IoError("short read");
  }

  // close
  const Status close = co_await file.Close();
  co_return Report(status.ok() ? close : status);
}

Future<Status> LocalFileSystem::Link(std::string from, std::string to) {
  const Status mkdir = co_await FileSystem::MakeDirs(
      std::filesystem::path(to).parent_path().string());
  if (!mkdir.ok()) {
    co_return mkdir;
  }
  co_return co_await FileSystem::Link(std::move(from), std::move(to));
}

Future<Status> LocalFileSystem::Unlink(std::string path) {
  co_return Report(co_await FileSystem::Unlink(std::move(path)));
}

Future<bool> LocalFileSystem::FileExists(std::string path) {
  co_return (co_await FileSystem::StatPath(std::move(path))).ok();
}

Status LocalFileSystem::CheckBlock(const BufferViews& block) {
  if (block.empty() || block.size() > kMaxBufferViews ||
      TotalBytes(block) == 0) {
    return Status::InvalidParam("a block is 1.." +
                                std::to_string(kMaxBufferViews) +
                                " non-empty ranges");
  }
  return Status::OK();
}

Future<StatusOr<File>> LocalFileSystem::OpenFile(std::string path,
                                                 OpenFlags flags, bool direct) {
  OpenOption option = BlockOpenOption();
  option.direct = direct;
  co_return co_await FileSystem::Open(std::move(path), flags, option);
}

Future<Status> LocalFileSystem::WriteFile(File* file, BufferViews block,
                                          bool direct) {
  const uint64_t total = TotalBytes(block);
  const uint64_t align_length = AlignUp4K(total);
  const auto length = static_cast<uint32_t>(direct ? align_length : total);

  const Status status = co_await file->Allocate(0, align_length);
  if (!status.ok()) {
    co_return status;
  }

  struct iovec iov[kMaxBufferViews];
  for (size_t i = 0; i < block.size(); ++i) {
    iov[i] = {.iov_base = block[i].data, .iov_len = block[i].size};
  }

  const auto n = static_cast<unsigned>(block.size());
  StatusOr<size_t> nwritten(0);
  if (n == 1) {
    nwritten = co_await file->Write(0, block[0].data, length);
  } else {
    nwritten = co_await file->Writev(0, iov, n, length);
  }

  if (!nwritten.ok()) {
    co_return nwritten.status();
  }
  co_return *nwritten == length ? Status::OK() : Status::IoError("short write");
}

Status LocalFileSystem::Report(Status status) {
  if (status.ok()) {
    health_->IoSuccess();
  } else if (!status.IsNotExist()) {
    health_->IoError();
  }
  return status;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
