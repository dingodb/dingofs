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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/filesystem.h"

#include <butil/iobuf.h>

#include <memory>

#include "cache/common/types.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/io_uring.h"
#include "cache/storage/aio/usrbio.h"
#include "cache/storage/buffer.h"
#include "cache/storage/closure.h"
#include "cache/utils/helper.h"
#include "cache/utils/page_cache_manager.h"
#include "cache/utils/posix.h"
#include "cache/utils/sys_conf.h"

namespace dingofs {
namespace cache {
namespace storage {

using dingofs::cache::utils::IsAligned;
using dingofs::cache::utils::Posix;
using dingofs::cache::utils::SysConf;

LocalFileSystem::LocalFileSystem(uint32_t io_depth)
    : io_depth_(io_depth),
      running_(false),
      io_ring_(std::make_shared<LinuxIOUring>(io_depth)),
      aio_queue_(std::make_unique<AioQueueImpl>(io_ring_)),
      page_cache_manager_(std::make_unique<utils::PageCacheManager>()) {}

Status LocalFileSystem::Init() {
  if (!running_.exchange(true)) {
    return aio_queue_->Init();
  }
  return Status::OK();
}

Status LocalFileSystem::Shutdown() {
  if (running_.exchange(false)) {
    return aio_queue_->Shutdown();
  }
  return Status::OK();
}

Status LocalFileSystem::Write(int fd, IOBuffer* buffer) {
  // TODO(Wine93): we should compare the peformance for below case which should
  // execute by io_uring or sync write:
  //  1. direct-io with one continuos buffer
  //  2. direct-io with multi buffers
  //  3. buffer-io with one continuos buffer
  //  4. buffer-io with multi buffers
  //
  // now select use way-2 or way-4 by io_uring.
  auto* closure =
      new AioClosure(AioType::kWrite, fd, 0, buffer->Size(), buffer);
  aio_queue_->Submit(closure);
  closure->Wait();
  return closure->status();
}

Status LocalFileSystem::Read(int fd, off_t offset, size_t length,
                             IOBuffer* buffer) {
  if (IsAligned(offset, SysConf::GetPageSize())) {
    return MapFile(fd, offset, length, buffer);
  }

  auto* closure = new AioClosure(AioType::kRead, fd, offset, length, buffer);
  aio_queue_->Submit(closure);
  closure->Wait();
  return closure->status();
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
    Status status = Posix::MUnMap(data, length);
    if (status.ok()) {
      // TODO(Wine93): lru page cache
      page_cache_manager_->DropPageCache(fd, offset, length);
    }
  };

  butil::IOBuf iobuf;
  iobuf.append_user_data(data, length, deleter);
  *buffer = IOBuffer(iobuf);

  return status;
}

HF3FS::HF3FS(const std::string& mountpoint, uint32_t blksize, uint32_t io_depth)
    : running_(false) {
  io_ring_w_ = std::make_shared<USRBIO>(mountpoint, blksize, io_depth, false);
  io_ring_r_ = std::make_shared<USRBIO>(mountpoint, blksize, io_depth, true);
  aio_queue_r_ = std::make_unique<AioQueueImpl>(io_ring_r_);
}

Status HF3FS::Init() {
  if (!running_.exchange(true)) {
    auto status = aio_queue_w_->Init();
    if (status.ok()) {
      status = aio_queue_r_->Init();
    }
    return status;
  }
  return Status::OK();
}

Status HF3FS::Shutdown() {
  if (running_.exchange(false)) {
    auto status = aio_queue_w_->Shutdown();
    if (status.ok()) {
      status = aio_queue_r_->Shutdown();
    }
    return status;
  }
  return Status::OK();
}

Status HF3FS::Write(int fd, IOBuffer* buffer) {
  auto* closure =
      new AioClosure(AioType::kWrite, fd, 0, buffer->Size(), buffer);
  aio_queue_w_->Submit(closure);
  closure->Wait();
  return closure->status();
}

Status HF3FS::Read(int fd, off_t offset, size_t length, IOBuffer* buffer) {
  auto* closure = new AioClosure(AioType::kRead, fd, offset, length, buffer);
  aio_queue_r_->Submit(closure);
  closure->Wait();
  return closure->status();
}

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
