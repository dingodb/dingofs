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
 * Created Date: 2025-05-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/local_filesystem.h"

#include <butil/memory/aligned_memory.h>
#include <butil/memory/scope_guard.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <atomic>
#include <cstddef>
#include <memory>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/aio_queue.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_health_checker.h"
#include "cache/common/macro.h"
#include "cache/iutil/buffer_pool.h"
#include "cache/iutil/file_util.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

LocalFileSystem::LocalFileSystem(DiskCacheLayoutSPtr layout)
    : running_(false),
      layout_(layout),
      write_buffer_pool_(std::make_unique<BufferPool>(4 * kMiB, FLAGS_iodepth,
                                                      kAlignedIOBlockSize)),
      read_buffer_pool_(std::make_unique<BufferPool>(4 * kMiB, FLAGS_iodepth,
                                                     kAlignedIOBlockSize)),
      aio_queue_(std::make_unique<AioQueue>(write_buffer_pool_->Fetch(),
                                            read_buffer_pool_->Fetch())),
      health_checker_(std::make_unique<DiskHealthChecker>(layout)) {}

Status LocalFileSystem::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "LocalFileSystem already started";
    return Status::OK();
  }

  LOG(INFO) << "LocalFileSystem is starting...";

  auto status = aio_queue_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start AioQueue";
    return status;
  }

  health_checker_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "LocalFileSystem started";
  return Status::OK();
}

Status LocalFileSystem::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "LocalFileSystem already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "LocalFileSystem is shutting down...";

  health_checker_->Shutdown();

  auto status = aio_queue_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown AioQueue";
    return status;
  }

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "LocalFilesystem is down";
  return Status::OK();
}

Status LocalFileSystem::WriteFile(ContextSPtr ctx, const std::string& path,
                                  const IOBuffer* buffer) {
  DCHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  auto tmppath = TempFilepath(path);
  status = iutil::MkDirs(iutil::ParentDir(tmppath));
  if (!status.ok() && !status.IsExist()) {
    LOG(ERROR) << "Fail to mkdirs `" << iutil::ParentDir(tmppath) << "'";
    return status;
  }

  int fd;
  status = iutil::OpenFile(tmppath, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT,
                           0644, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to open file=`" << tmppath << "'";
    return status;
  }

  BRPC_SCOPE_EXIT {
    iutil::Close(fd);
    if (!status.ok()) {
      iutil::Unlink(tmppath);
    }
  };

  size_t aligned_length = AlignLength(buffer->Size());
  if (buffer->Size() != aligned_length) {
    status = iutil::Fallocate(fd, 0, 0, aligned_length);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to fallocate file=`" << path << "'";
      return status;
    }
  }

  auto* fixed_buffer = write_buffer_pool_->Alloc();
  buffer->CopyTo(fixed_buffer);

  IOBuffer write_buffer;
  write_buffer.AppendUserData(fixed_buffer, aligned_length, [this](void* ptr) {
    write_buffer_pool_->Free((char*)ptr);
  });

  status = AioWrite(ctx, fd, fixed_buffer, aligned_length,
                    write_buffer_pool_->Index(fixed_buffer));
  if (!status.ok()) {
    LOG(ERROR) << "Fail to write file'`" << tmppath << "'";
    return status;
  }

  status = iutil::Rename(tmppath, path);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to rename file from `" << tmppath << "' to `" << path
               << "'";
    return status;
  }
  return status;
}

Status LocalFileSystem::ReadFile(ContextSPtr ctx, const std::string& path,
                                 off_t offset, size_t length,
                                 IOBuffer* buffer) {
  CHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  int fd;
  status = iutil::OpenFile(path, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to open file";
    return status;
  }

  BRPC_SCOPE_EXIT { iutil::Close(fd); };

  off_t aligned_offset = AlignOffset(offset);
  size_t aligned_length = AlignLength(length);

  auto* fixed_buffer = read_buffer_pool_->Alloc();
  buffer->AppendUserData(fixed_buffer, aligned_length, [this](void* ptr) {
    read_buffer_pool_->Free((char*)ptr);
  });

  status = AioRead(ctx, fd, aligned_offset, aligned_length, fixed_buffer,
                   read_buffer_pool_->Index(fixed_buffer));
  if (status.ok()) {
    if (aligned_offset != offset) {
      buffer->PopFront(offset - aligned_offset);
    }
    if (aligned_length != length) {
      buffer->PopBack(aligned_offset + aligned_length - (offset + length));
    }
  } else {
    LOG(ERROR) << "Fail to read file=`" << path << "'";
  }

  return status;
}

Status LocalFileSystem::AioWrite(ContextSPtr ctx, int fd, char* buffer,
                                 size_t length, int buf_index) {
  auto aio = Aio(ctx, fd, 0, length, buffer, buf_index, false);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.status();
}

Status LocalFileSystem::AioRead(ContextSPtr ctx, int fd, off_t offset,
                                size_t length, char* buffer, int buf_index) {
  auto aio = Aio(ctx, fd, offset, length, buffer, buf_index, true);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.status();
}

off_t LocalFileSystem::AlignOffset(off_t offset) {
  auto alignment = kAlignedIOBlockSize;
  if (!IsAligned(offset, alignment)) {
    offset = offset - (offset % alignment);
  }
  return offset;
}

size_t LocalFileSystem::AlignLength(size_t length) {
  auto alignment = kAlignedIOBlockSize;
  if (!IsAligned(length, alignment)) {
    length = (length + alignment - 1) & ~(alignment - 1);
  }
  return length;
}

}  // namespace cache
}  // namespace dingofs
