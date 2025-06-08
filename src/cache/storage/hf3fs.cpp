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

#include "absl/cleanup/cleanup.h"
#include "cache/common/common.h"
#include "cache/config/config.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/usrbio.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"

namespace dingofs {
namespace cache {

HF3FS::HF3FS(const std::string& mountpoint, CheckStatusFunc check_status_func)
    : FileSystemBase(check_status_func),
      running_(false),
      io_ring_w_(std::make_shared<USRBIO>(mountpoint, FLAGS_ioring_blksize,
                                          FLAGS_ioring_iodepth, false)),
      io_ring_r_(std::make_shared<USRBIO>(mountpoint, FLAGS_ioring_blksize,
                                          FLAGS_ioring_iodepth, true)),
      aio_queue_w_(std::make_unique<AioQueueImpl>(io_ring_w_)),
      aio_queue_r_(std::make_unique<AioQueueImpl>(io_ring_r_)) {}

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

Status HF3FS::Destroy() {
  if (running_.exchange(false)) {
    auto status = aio_queue_w_->Shutdown();
    if (status.ok()) {
      status = aio_queue_r_->Shutdown();
    }
    return status;
  }
  return Status::OK();
}

Status HF3FS::WriteFile(const std::string& path, const IOBuffer& buffer,
                        WriteOption /*option*/) {
  int fd;
  auto tmpfile = Helper::TempFilepath(path);
  auto status = Posix::Creat(tmpfile, 0644, &fd);
  if (!status.ok()) {
    return CheckStatus(status);
  }

  auto defer = absl::MakeCleanup([&]() {
    Posix::Close(fd);
    if (!status.ok()) {
      Posix::Unlink(tmpfile);
    }
  });

  status = AioWrite(fd, buffer);
  if (status.ok()) {
    status = Posix::Rename(tmpfile, path);
  }
  return CheckStatus(status);
}

Status HF3FS::ReadFile(const std::string& path, off_t offset, size_t length,
                       IOBuffer* buffer, ReadOption /*option*/) {
  int fd;
  auto status = Posix::Open(path, O_RDONLY, &fd);
  if (status.ok()) {
    auto defer = absl::MakeCleanup([fd]() { Posix::Close(fd); });
    status = AioRead(fd, offset, length, buffer);
  }
  return CheckStatus(status);
}

Status HF3FS::AioWrite(int fd, const IOBuffer& buffer) {
  auto closure = AioWriteClosure(fd, buffer);
  aio_queue_w_->Submit(&closure);
  closure.Wait();
  return closure.status();
}

Status HF3FS::AioRead(int fd, off_t offset, size_t length, IOBuffer* buffer) {
  auto closure = AioReadClosure(fd, offset, length, buffer);
  aio_queue_r_->Submit(&closure);
  closure.Wait();
  return closure.status();
}

}  // namespace cache
}  // namespace dingofs
