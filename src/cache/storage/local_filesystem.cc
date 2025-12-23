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

#include "cache/storage/local_filesystem.h"

#include <butil/memory/aligned_memory.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <cstddef>
#include <memory>
#include <utility>

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/common/type.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/linux_io_uring.h"
#include "cache/storage/base_filesystem.h"
#include "cache/storage/filesystem.h"
#include "cache/utils/buffer_pool.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/offload_thread_pool.h"
#include "cache/utils/posix.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

static const std::string kModule = "filesystem";
static const constexpr size_t kBlkSize = 4 * kMiB;

LocalFileSystem::LocalFileSystem(CheckStatusFunc check_status_func)
    : BaseFileSystem::BaseFileSystem(check_status_func),
      running_(false),
      buffer_pool_(std::make_shared<BufferPool>(FLAGS_ioring_iodepth * kBlkSize,
                                                Helper::GetIOAlignedBlockSize(),
                                                kBlkSize)),
      io_ring_(std::make_shared<LinuxIOUring>(FLAGS_ioring_iodepth,
                                              buffer_pool_->RawBuffer())),
      aio_queue_(std::make_unique<AioQueueImpl>(io_ring_)),
      page_cache_manager_(std::make_unique<PageCacheManager>()) {}

Status LocalFileSystem::Start() {
  CHECK_NOTNULL(buffer_pool_);
  CHECK_NOTNULL(io_ring_);
  CHECK_NOTNULL(aio_queue_);
  CHECK_NOTNULL(page_cache_manager_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Local filesystem is starting...";

  auto status = io_ring_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start io ring failed: " << status.ToString();
    return status;
  }

  status = status = aio_queue_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start aio queue failed: " << status.ToString();
    return status;
  }

  status = page_cache_manager_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start page cache manager failed: " << status.ToString();
    return status;
  }

  running_ = true;

  LOG(INFO) << "Local filesystem is up.";

  CHECK_RUNNING("Local filesystem");
  return Status::OK();
}

Status LocalFileSystem::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Local filesystem is shutting down...";

  auto status = aio_queue_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown aio queue failed: " << status.ToString();
    return status;
  }

  status = io_ring_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown io ring failed: " << status.ToString();
    return status;
  }

  status = page_cache_manager_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown page cache manager failed: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Local filesystem is down.";

  CHECK_DOWN("Local filesystem");
  return Status::OK();
}

off_t LocalFileSystem::AlignOffset(off_t offset) {
  auto alignment = Helper::GetIOAlignedBlockSize();
  if (!Helper::IsAligned(offset, alignment)) {
    offset = offset - (offset % alignment);
  }
  return offset;
}

size_t LocalFileSystem::AlignLength(size_t length) {
  auto alignment = Helper::GetIOAlignedBlockSize();
  if (!Helper::IsAligned(length, alignment)) {
    length = (length + alignment - 1) & ~(alignment - 1);
  }
  return length;
}

std::pair<off_t, size_t> LocalFileSystem::AlignRequest(off_t offset,
                                                       size_t length) {
  auto alignment = Helper::GetIOAlignedBlockSize();
  off_t roffset = AlignOffset(offset);
  size_t rlength = AlignLength(length + (offset % alignment));
  return std::make_pair(roffset, rlength);
}

// TODO(Wine93): we should compare the peformance for below case which
// should execute by io_uring or sync write:
//  1. direct-io with one continuos buffer
//  2. direct-io with multi buffers
//  3. buffer-io with one continuos buffer
//  4. buffer-io with multi buffers
//
// now we use way-2 or way-4 by io_uring.
Status LocalFileSystem::WriteFile(ContextSPtr ctx, const std::string& path,
                                  const IOBuffer& buffer, WriteOption option) {
  CHECK_RUNNING("Local filesystem");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "write(%s,%zu)",
                    Helper::Filename(path), buffer.Size());
  StepTimerGuard guard(timer);

  NEXT_STEP("mkdir");
  auto tmppath = Helper::TempFilepath(path);
  status = MkDirs(Helper::ParentDir(tmppath));
  if (!status.ok() && !status.IsExist()) {
    LOG_CTX(ERROR) << "Mkdir failed: path = " << Helper::ParentDir(tmppath)
                   << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  // TODO: fallocate and split IO (1MB * 4)
  NEXT_STEP("open");
  int fd;
  int flags = Posix::kDefaultCreatFlags;
  if (option.direct_io &&
      Helper::IsAligned(buffer.Size(), Helper::GetIOAlignedBlockSize())) {
    flags |= O_DIRECT;
  } else {
    option.direct_io = false;
  }
  status = Posix::Open(tmppath, flags, 0644, &fd);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Open file failed: path = " << path
                   << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  SCOPE_EXIT {
    if (!status.ok()) {
      Unlink(ctx, tmppath);
    }
  };

  if (!Helper::IsAligned(buffer.Size(), Helper::GetIOAlignedBlockSize())) {
    NEXT_STEP("fallocate");
    status = Posix::Fallocate(fd, 0, 0, AlignLength(buffer.Size()));
    if (!status.ok()) {
      LOG_CTX(ERROR) << "Fail to fallocate file=" << path
                     << ", status=" << status.ToString();
      return status;
    }
  }

  NEXT_STEP("memcpy");
  IOBuffer wbuf;  // write buffer
  if (option.direct_io) {
    wbuf = CopyBuffer(buffer);
    option.fixed_buffer_index = buffer_pool_->Index(wbuf.Fetch1());
  } else {
    wbuf = buffer;
  }

  NEXT_STEP("aio_write");
  status = AioWrite(ctx, fd, &wbuf, option);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Aio write failed: path = " << tmppath
                   << ", length = " << wbuf.Size()
                   << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  NEXT_STEP("rename");
  status = Posix::Rename(tmppath, path);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Rename file failed: from = " << tmppath
                   << ", to = " << path << ", status = " << status.ToString();
  }
  return CheckStatus(status);
}

Status LocalFileSystem::ReadFile(ContextSPtr ctx, const std::string& path,
                                 off_t offset, size_t length, IOBuffer* buffer,
                                 ReadOption option) {
  CHECK_RUNNING("Local filesystem");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "read(%s,%zu,%zu)",
                    Helper::Filename(path), offset, length);
  StepTimerGuard guard(timer);

  NEXT_STEP("open");
  int fd;
  status = Posix::Open(path, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Open file failed: path = " << path
                   << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  NEXT_STEP("aio_read");
  auto align = AlignRequest(offset, length);
  off_t roffset = align.first;
  size_t rlength = align.second;
  status = AioRead(ctx, fd, roffset, rlength, buffer, option);
  if (status.ok()) {
    if (roffset != offset) {
      buffer->PopFront(offset - roffset);
    }
    if (rlength != length) {
      buffer->PopBack(roffset + rlength - (offset + length));
    }
  } else {
    LOG_CTX(ERROR) << "Aio read failed: path = " << path
                   << ", offset = " << offset << ", length = " << length
                   << ", status = " << status.ToString();
  }

  return CheckStatus(status);
}

Status LocalFileSystem::AioWrite(ContextSPtr ctx, int fd, IOBuffer* buffer,
                                 WriteOption option) {
  auto aio =
      Aio(ctx, fd, 0, buffer->Size(), buffer, false, option.fixed_buffer_index);
  aio_queue_->Submit(&aio);
  aio.Wait();

  auto status = aio.status();
  if (!status.ok()) {
    CloseFd(ctx, fd);
    return status;
  }

  if (!option.direct_io && option.drop_page_cache) {
    page_cache_manager_->AsyncDropPageCache(ctx, fd, 0, buffer->Size());
  } else {
    CloseFd(ctx, fd);
  }
  return status;
}

Status LocalFileSystem::AioRead(ContextSPtr ctx, int fd, off_t offset,
                                size_t length, IOBuffer* buffer,
                                ReadOption /*option*/) {
  auto aio = Aio(ctx, fd, offset, length, buffer, true);
  aio_queue_->Submit(&aio);
  aio.Wait();

  auto status = aio.status();
  CloseFd(ctx, fd);
  return status;
}

Status LocalFileSystem::MapFile(ContextSPtr ctx, int fd, off_t offset,
                                size_t length, IOBuffer* buffer,
                                ReadOption option) {
  void* data;
  auto status =
      Posix::MMap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset, &data);
  if (!status.ok()) {
    CloseFd(ctx, fd);
    return status;
  }

  auto deleter = [this, ctx, fd, offset, length, option](void* data) {
    auto status = Posix::MUnmap(data, length);
    if (!status.ok()) {
      LOG_CTX(ERROR) << "MUnmap failed: fd = " << fd << ", offset = " << offset
                     << ", length = " << length
                     << ", status = " << status.ToString();
      CloseFd(ctx, fd);
      return;
    }

    if (option.drop_page_cache) {  // it will close fd
      page_cache_manager_->AsyncDropPageCache(ctx, fd, offset, length);
    } else {
      CloseFd(ctx, fd);
    }
  };

  buffer->AppendUserData(data, length, deleter);
  return Status::OK();
}

void LocalFileSystem::CloseFd(ContextSPtr ctx, int fd) {
  auto status = Posix::Close(fd);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Close fd failed: fd = " << fd
                   << ", status = " << status.ToString();
  }
}

void LocalFileSystem::Unlink(ContextSPtr ctx, const std::string& path) {
  auto status = Posix::Unlink(path);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Unlink file failed: path = " << path
                   << ", status = " << status.ToString();
  }
}

IOBuffer LocalFileSystem::CopyBuffer(const IOBuffer& src) {
  IOBuffer dest;

  BthreadCountdownEvent countdown(1);
  OffloadThreadPool::Submit([&]() {
    char* data = buffer_pool_->Alloc();
    src.CopyTo(data);
    dest.AppendUserData(data, src.Size(),
                        [this, data](void*) { buffer_pool_->Free(data); });
    countdown.signal(1);
  });
  countdown.wait();

  return dest;
}

}  // namespace cache
}  // namespace dingofs
