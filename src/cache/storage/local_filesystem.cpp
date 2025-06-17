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

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/config/blockcache.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/linux_io_uring.h"
#include "cache/storage/base_filesystem.h"
#include "cache/storage/filesystem.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

LocalFileSystem::LocalFileSystem(CheckStatusFunc check_status_func)
    : BaseFileSystem::BaseFileSystem(check_status_func),
      running_(false),
      io_ring_(std::make_shared<LinuxIOUring>(FLAGS_ioring_iodepth)),
      aio_queue_(std::make_unique<AioQueueImpl>(io_ring_)),
      page_cache_manager_(std::make_unique<PageCacheManager>()) {}

Status LocalFileSystem::Start() {
  CHECK_NOTNULL(io_ring_);
  CHECK_NOTNULL(aio_queue_);
  CHECK_NOTNULL(page_cache_manager_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Local filesystem is starting...";

  auto status = page_cache_manager_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start page cache manager failed: " << status.ToString();
    return status;
  }

  status = status = aio_queue_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start AIO queue failed: " << status.ToString();
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
    LOG(ERROR) << "Shutdown AIO queue failed: " << status.ToString();
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

  NEXT_STEP(kMkDir);
  auto tmppath = Helper::TempFilepath(path);
  auto status = MkDirs(Helper::ParentDir(tmppath));
  if (!status.ok() && !status.IsExist()) {
    LOG(ERROR) << "Create parent directory failed: path = " << tmppath
               << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  NEXT_STEP(kOpenFile);
  int fd;
  bool use_direct = Helper::IsAligned(buffer);
  int flags = Posix::kDefaultCreatFlags | (use_direct ? O_DIRECT : 0);
  status = Posix::Open(tmppath, flags, 0644, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Open file failed: path = " << tmppath
               << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  NEXT_STEP(kAioWrite);
  status = AioWrite(ctx, fd, buffer, option);
  if (!status.ok()) {
    LOG(ERROR) << "Aio write failed: path = " << tmppath
               << ", status = " << status.ToString();
    Unlink(tmppath);
    return CheckStatus(status);
  }

  NEXT_STEP(kRenameFile);
  status = Posix::Rename(tmppath, path);
  if (!status.ok()) {
    LOG(ERROR) << "Rename temp file failed: from = " << tmppath
               << ", to = " << path << ", status = " << status.ToString();
    Unlink(tmppath);
  }
  return CheckStatus(status);
}

Status LocalFileSystem::ReadFile(ContextSPtr ctx, const std::string& path,
                                 off_t offset, size_t length, IOBuffer* buffer,
                                 ReadOption option) {
  CHECK_RUNNING("Local filesystem");

  NEXT_STEP(kCreatFile);
  int fd;
  auto status = Posix::Open(path, O_RDONLY, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Open file failed: path = " << path
               << ", status = " << status.ToString();
    return CheckStatus(status);
  }

  if (Helper::IsAligned(offset, Helper::GetSysPageSize())) {
    NEXT_STEP(kMmap);
    status = MapFile(ctx, fd, offset, length, buffer, option);
  } else {
    NEXT_STEP(kAioRead);
    status = AioRead(ctx, fd, offset, length, buffer, option);
  }

  if (!status.ok()) {
    LOG(ERROR) << "Read file failed: path = " << path << ", offset = " << offset
               << ", length = " << length << ", status = " << status.ToString();
  }
  return CheckStatus(status);
}

Status LocalFileSystem::AioWrite(ContextSPtr ctx, int fd,
                                 const IOBuffer& buffer, WriteOption option) {
  auto aio = Aio(ctx, fd, 0, buffer.Size(),
                 const_cast<IOBuffer*>(&buffer));  // FIXME: remove const_castp
  aio_queue_->Submit(&aio);
  aio.Wait();

  auto status = aio.status();
  if (status.ok() && option.drop_page_cache) {
    page_cache_manager_->AsyncDropPageCache(ctx, fd, 0, buffer.Size());
  } else {
    CloseFd(fd);
  }
  return status;
}

Status LocalFileSystem::AioRead(ContextSPtr ctx, int fd, off_t offset,
                                size_t length, IOBuffer* buffer,
                                ReadOption option) {
  auto aio = Aio(ctx, fd, offset, length, buffer, true);
  aio_queue_->Submit(&aio);
  aio.Wait();

  auto status = aio.status();
  if (status.ok() && option.drop_page_cache) {
    page_cache_manager_->AsyncDropPageCache(ctx, fd, offset, length);
  } else {
    CloseFd(fd);
  }
  return status;
}

Status LocalFileSystem::MapFile(ContextSPtr ctx, int fd, off_t offset,
                                size_t length, IOBuffer* buffer,
                                ReadOption option) {
  void* data;
  auto status =
      Posix::MMap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset, &data);
  if (!status.ok()) {
    CloseFd(fd);
    return status;
  }

  auto deleter = [this, ctx, fd, offset, length, option](void* data) {
    auto status = Posix::MUnmap(data, length);
    if (status.ok() && option.drop_page_cache) {  // it will close fd
      page_cache_manager_->AsyncDropPageCache(ctx, fd, offset, length);
    } else {
      CloseFd(fd);
      LOG(ERROR) << "MUnmap failed: fd = " << fd << ", offset = " << offset
                 << ", length = " << length
                 << ", status = " << status.ToString();
    }
  };

  butil::IOBuf iobuf;
  iobuf.append_user_data(data, length, deleter);
  *buffer = IOBuffer(iobuf);
  return Status::OK();
}

void LocalFileSystem::CloseFd(int fd) {
  auto status = Posix::Close(fd);
  if (!status.ok()) {
    LOG(ERROR) << "Close file descriptor failed: fd = " << fd
               << ", status = " << status.ToString();
  }
}

void LocalFileSystem::Unlink(const std::string& path) {
  auto status = Posix::Unlink(path);
  if (!status.ok()) {
    LOG(ERROR) << "Unlink file failed: path = " << path
               << ", status = " << status.ToString();
  }
}

}  // namespace cache
}  // namespace dingofs
