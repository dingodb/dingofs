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

#include <memory>

#include "absl/cleanup/cleanup.h"
#include "cache/config/config.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/linux_io_uring.h"
#include "cache/storage/filesystem_base.h"
#include "cache/utils/filepath.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"

namespace dingofs {
namespace cache {

LocalFileSystem::LocalFileSystem(CheckStatusFunc check_status_func)
    : FileSystemBase::FileSystemBase(check_status_func),
      running_(false),
      io_ring_(std::make_shared<LinuxIOUring>(FLAGS_ioring_iodepth)),
      aio_queue_(std::make_unique<AioQueueImpl>(io_ring_)),
      page_cache_manager_(std::make_unique<PageCacheManager>()) {}

Status LocalFileSystem::Init() {
  if (!running_.exchange(true)) {
    auto status = page_cache_manager_->Start();
    if (status.ok()) {
      status = aio_queue_->Init();
    }
    return status;
  }
  return Status::OK();
}

Status LocalFileSystem::Destroy() {
  if (running_.exchange(false)) {
    auto status = aio_queue_->Shutdown();
    if (status.ok()) {
      status = page_cache_manager_->Stop();
    }
    return status;
  }
  return Status::OK();
}

Status LocalFileSystem::WriteFile(const std::string& path,
                                  const IOBuffer& buffer, WriteOption option) {
  // TODO(Wine93): we should compare the peformance for below case which
  // should execute by io_uring or sync write:
  //  1. direct-io with one continuos buffer
  //  2. direct-io with multi buffers
  //  3. buffer-io with one continuos buffer
  //  4. buffer-io with multi buffers
  //
  // now we use way-2 or way-4 by io_uring.
  int fd;
  auto tmppath = Helper::TempFilepath(path);
  auto status = MkDirs(FilePath::ParentDir(tmppath));
  if (status.ok() || status.IsExist()) {
    status = Posix::Creat(tmppath, 0644, &fd);
  }
  if (!status.ok()) {
    return CheckStatus(status);
  }

  auto defer = absl::MakeCleanup([&]() {
    if (option.drop_page_cache) {
      auto st = Posix::PosixFAdvise(fd, 0, 0, POSIX_FADV_DONTNEED);
      if (!st.ok()) {
        LOG(ERROR) << "drop page cache failed: " << st.ToString();
      }
    }
    Posix::Close(fd);
    if (!status.ok()) {
      Posix::Unlink(tmppath);
    }
  });

  status = AioWrite(fd, buffer);
  if (status.ok()) {
    status = Posix::Rename(tmppath, path);
  }

  return CheckStatus(status);
}

Status LocalFileSystem::ReadFile(const std::string& path, off_t offset,
                                 size_t length, IOBuffer* buffer,
                                 ReadOption /*option*/) {
  int fd;
  auto status = Posix::Open(path, O_RDONLY, &fd);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  auto defer = absl::MakeCleanup([fd]() { Posix::Close(fd); });
  if (Helper::IsAligned(offset, Helper::GetSysPageSize())) {
    status = MapFile(fd, offset, length, buffer);
  } else {
    status = AioRead(fd, offset, length, buffer);
  }
  return CheckStatus(status);
}

Status LocalFileSystem::AioWrite(int fd, const IOBuffer& buffer) {
  auto aio = AioWriteClosure(fd, buffer);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.status();
}

Status LocalFileSystem::AioRead(int fd, off_t offset, size_t length,
                                IOBuffer* buffer) {
  auto aio = AioReadClosure(fd, offset, length, buffer);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.status();
}

Status LocalFileSystem::MapFile(int fd, off_t offset, size_t length,
                                IOBuffer* buffer) {
  void* data;
  auto status =
      Posix::MMap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset, &data);
  if (!status.ok()) {
    return status;
  }

  auto deleter = [this, fd, offset, length](void* data) {
    auto status = Posix::MUnMap(data, length);
    if (status.ok()) {
      page_cache_manager_->DropPageCache(fd, offset, length);
    }
  };

  butil::IOBuf iobuf;
  iobuf.append_user_data(data, length, deleter);
  *buffer = IOBuffer(iobuf);
  return status;
}

}  // namespace cache
}  // namespace dingofs
