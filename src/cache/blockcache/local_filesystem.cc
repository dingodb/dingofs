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

#include <bits/types/struct_iovec.h>
#include <butil/memory/aligned_memory.h>
#include <butil/memory/scope_guard.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/aio_queue.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_health_checker.h"
#include "cache/common/macro.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/slab_pool.h"
#include "cache/iutil/buffer_pool.h"
#include "cache/iutil/file_util.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

FixedBuffers::FixedBuffers()
    : write_pool_(infiniband::GetGlobalReadSlabPool()),
      read_pool_(infiniband::GetGlobalWriteSlabPool()) {}

Status FixedBuffers::Alloc(size_t size, bool for_read, IOBuffer* buffer,
                           int* buf_index) {
  infiniband::RDMABufferPool* pool = for_read ? read_pool_ : write_pool_;
  auto* rdma_buffer = pool->Alloc();
  if (rdma_buffer == nullptr) {
    return Status::OutOfMemory("out of memory");
  }

  buffer->AppendUserDataWithMeta(
      rdma_buffer->data, size,
      [pool](void* addr) {
        pool->Free(reinterpret_cast<infiniband::RDMABuffer*>(addr));
      },
      rdma_buffer->lkey);

  *buf_index = GetIndex(rdma_buffer, for_read);
  return Status::OK();
}

bool FixedBuffers::IsFixed(const IOBuffer* buffer, bool for_read,
                           int* buf_index) {
  // A fixed buffer is a single contiguous block carved from a slab pool, so a
  // multi-block buffer can never be one.
  if (buffer->ConstIOBuf().backing_block_num() != 1) {
    *buf_index = -1;
    return false;
  }

  infiniband::RDMABufferPool* pool = for_read ? read_pool_ : write_pool_;
  int index = pool->IndexOf(buffer->Fetch1());
  if (index < 0) {
    *buf_index = -1;
    return false;
  }

  *buf_index = for_read ? write_pool_->BufferCount() + index : index;
  return true;
}

int FixedBuffers::GetIndex(infiniband::RDMABuffer* rdma_buffer, bool for_read) {
  if (!for_read) {
    return write_pool_->IndexOf(rdma_buffer);
  }
  return write_pool_->BufferCount() + read_pool_->IndexOf(rdma_buffer);
}

std::vector<iovec> FixedBuffers::Fetch() {
  auto write_buffers = write_pool_->Fetch();
  auto read_buffers = write_pool_->Fetch();

  std::vector<iovec> buffers;
  buffers.reserve(write_buffers.size() + read_buffers.size());
  buffers.insert(buffers.end(), write_buffers.begin(), write_buffers.end());
  buffers.insert(buffers.end(), read_buffers.begin(), read_buffers.end());

  return buffers;
}

struct InflightAioGuard {
  InflightAioGuard(int fd, iutil::InflightTracker* inflight)
      : fd(fd), inflight(inflight) {
    CHECK(inflight->Add(std::to_string(fd)).ok());
  }

  ~InflightAioGuard() { inflight->Remove(std::to_string(fd)); }

  int fd;
  iutil::InflightTracker* inflight;
};

LocalFileSystem::LocalFileSystem(DiskCacheLayoutSPtr layout)
    : running_(false),
      layout_(layout),
      fixed_buffers_(std::make_unique<FixedBuffers>()),
      inflight_(FLAGS_iodepth),
      aio_queue_(std::make_unique<AioQueue>(fixed_buffers_->Fetch())),
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

Status LocalFileSystem::WriteFile(const std::string& path,
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

  IOBuffer fixed;
  IOBuffer* write_buffer;
  int buf_index;
  if (fixed_buffers_->IsFixed(buffer, false, &buf_index)) {
    write_buffer = const_cast<IOBuffer*>(buffer);
  } else {
    status = fixed_buffers_->Alloc(aligned_length, false, &fixed, &buf_index);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to allocate fixed write buffer for `" << path << "'";
      return status;
    }
    buffer->AppendTo(&fixed);
    write_buffer = &fixed;
  }

  status = AioWrite(fd, write_buffer->Fetch1(), aligned_length, buf_index);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to write file'`" << tmppath << "'";
    return status;
  }

  status = iutil::Rename(tmppath, path);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to rename `" << tmppath << "' to `" << path << "'";
    return status;
  }
  return status;
}

Status LocalFileSystem::ReadFile(const std::string& path, off_t offset,
                                 size_t length, IOBuffer* buffer) {
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
  size_t aligned_length = AlignLength(length + offset - aligned_offset);

  IOBuffer aligned;
  int buf_index;
  status = fixed_buffers_->Alloc(aligned_length, true, &aligned, &buf_index);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to allocate read buffer for `" << path << "'";
    return status;
  }

  status =
      AioRead(fd, aligned_offset, aligned_length, aligned.Fetch1(), buf_index);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to read file=`" << path << "'";
    return status;
  }

  if (aligned_offset != offset) {
    aligned.PopFront(offset - aligned_offset);
  }
  if (aligned_length != length) {
    aligned.PopBack(aligned_offset + aligned_length - (offset + length));
  }

  if (buffer->Size() == 0) {
    *buffer = std::move(aligned);
  } else {
    CHECK_EQ(buffer->BackingBlockNum(), 1);
    CHECK_GE(buffer->Size(), length);
    aligned.CopyTo(buffer->Fetch1(), length);
  }
  return status;
}

Status LocalFileSystem::AioWrite(int fd, char* buffer, size_t length,
                                 int buf_index) {
  InflightAioGuard guard(fd, &inflight_);

  auto aio = Aio(fd, 0, length, buffer, buf_index, false);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.Result().status;
}

Status LocalFileSystem::AioRead(int fd, off_t offset, size_t length,
                                char* buffer, int buf_index) {
  InflightAioGuard guard(fd, &inflight_);

  auto aio = Aio(fd, offset, length, buffer, buf_index, true);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.Result().status;
}

bool LocalFileSystem::IsAligned(uint64_t n, uint64_t m) { return (n % m) == 0; }

off_t LocalFileSystem::AlignOffset(off_t offset) {
  if (!IsAligned(offset, kAlignedIOBlockSize)) {
    offset = offset - (offset % kAlignedIOBlockSize);
  }
  return offset;
}

size_t LocalFileSystem::AlignLength(size_t length) {
  if (!IsAligned(length, kAlignedIOBlockSize)) {
    length = (length + kAlignedIOBlockSize - 1) & ~(kAlignedIOBlockSize - 1);
  }
  return length;
}

}  // namespace cache
}  // namespace dingofs
