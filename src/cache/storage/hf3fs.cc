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

#include "cache/storage/hf3fs.h"

#include "cache/common/macro.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/usrbio.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(ioring_blksize, 1048576,
              "Block size for iouring operations in bytes");

HF3FS::HF3FS(const std::string& mountpoint, CheckStatusFunc check_status_func)
    : BaseFileSystem(check_status_func),
      running_(false),
      io_ring_w_(std::make_shared<USRBIO>(mountpoint, FLAGS_ioring_blksize,
                                          FLAGS_ioring_iodepth, false)),
      io_ring_r_(std::make_shared<USRBIO>(mountpoint, FLAGS_ioring_blksize,
                                          FLAGS_ioring_iodepth, true)),
      aio_queue_w_(std::make_unique<AioQueueImpl>(io_ring_w_)),
      aio_queue_r_(std::make_unique<AioQueueImpl>(io_ring_r_)) {}

Status HF3FS::Start() {
  if (!running_.exchange(true)) {
    auto status = aio_queue_w_->Start();
    if (status.ok()) {
      status = aio_queue_r_->Start();
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

Status HF3FS::WriteFile(ContextSPtr ctx, const std::string& path,
                        const IOBuffer& buffer, WriteOption /*option*/) {
  int fd;
  auto tmpfile = Helper::TempFilepath(path);
  auto status = Posix::Creat(tmpfile, 0644, &fd);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  SCOPE_EXIT {
    Posix::Close(fd);
    if (!status.ok()) {
      Posix::Unlink(tmpfile);
    }
  };

  status = AioWrite(ctx, fd, buffer);
  if (status.ok()) {
    status = Posix::Rename(tmpfile, path);
  }
  return CheckStatus(status);
}

Status HF3FS::ReadFile(ContextSPtr ctx, const std::string& path, off_t offset,
                       size_t length, IOBuffer* buffer, ReadOption /*option*/) {
  int fd;
  auto status = Posix::Open(path, O_RDONLY, &fd);
  if (status.ok()) {
    SCOPE_EXIT { Posix::Close(fd); };
    status = AioRead(ctx, fd, offset, length, buffer);
  }
  return CheckStatus(status);
}

Status HF3FS::AioWrite(ContextSPtr ctx, int fd, const IOBuffer& buffer) {
  auto closure =
      Aio(ctx, fd, 0, buffer.Size(), const_cast<IOBuffer*>(&buffer), false);
  aio_queue_w_->Submit(&closure);
  closure.Wait();
  return closure.status();
}

Status HF3FS::AioRead(ContextSPtr ctx, int fd, off_t offset, size_t length,
                      IOBuffer* buffer) {
  auto closure = Aio(ctx, fd, offset, length, buffer, true);
  aio_queue_r_->Submit(&closure);
  closure.Wait();
  return closure.status();
}

}  // namespace cache
}  // namespace dingofs
