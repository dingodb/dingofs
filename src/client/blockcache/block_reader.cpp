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
 * Created Date: 2025-03-30
 * Author: Jingli Chen (Wine93)
 */

#include "client/blockcache/block_reader.h"

#include <glog/logging.h>

#include <cstring>

#include "base/math/math.h"
#include "client/blockcache/aio_queue.h"
#include "client/blockcache/error.h"

namespace dingofs {
namespace client {
namespace blockcache {

using base::math::kKiB;

LocalBlockReader::LocalBlockReader(int fd, std::shared_ptr<LocalFileSystem> fs)
    : fd_(fd), fs_(fs) {}

BCACHE_ERROR LocalBlockReader::ReadAt(off_t offset, size_t length,
                                      char* buffer) {
  return fs_->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    BCACHE_ERROR rc;
    rc = posix->LSeek(fd_, offset, SEEK_SET);
    if (rc == BCACHE_ERROR::OK) {
      rc = posix->Read(fd_, buffer, length);
    }
    return rc;
  });
}

void LocalBlockReader::Close() {
  fs_->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    posix->Close(fd_);
    return BCACHE_ERROR::OK;
  });
}

class AioClosure : public Closure {
 public:
  AioClosure(BlockTask* task, char* buffer) : task_(task), buffer_(buffer) {}

  void Run() override {
    if (Code() == BCACHE_ERROR::OK) {
      std::memcpy(buffer_, task_->iov.base, task_->length);
    }
    delete task_;
  }

 private:
  char* buffer_;
  BlockTask* task_;
};

RemoteBlockReader::RemoteBlockReader(int fd,
                                     std::shared_ptr<LocalFileSystem> fs,
                                     std::shared_ptr<AioQueue> aio_queue)
    : fd_(fd), fs_(fs), aio_queue_(aio_queue) {}

BCACHE_ERROR RemoteBlockReader::ReadAt(off_t offset, size_t length,
                                       char* buffer) {
  BlockTask* task;
  AioClosure closure(task, buffer);
  task = new BlockTask(fd_, offset, length, 4 * kKiB, &closure);
  aio_queue_->Submit(task);
  return closure.Code();
}

void RemoteBlockReader::Close() {
  fs_->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    posix->Close(fd_);
    return BCACHE_ERROR::OK;
  });
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
