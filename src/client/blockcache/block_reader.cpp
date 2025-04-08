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

#include <butil/time.h>
#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstring>

#include "client/blockcache/aio.h"
#include "client/common/status.h"

namespace dingofs {
namespace client {
namespace blockcache {

LocalBlockReader::LocalBlockReader(int fd, std::shared_ptr<LocalFileSystem> fs)
    : fd_(fd), fs_(fs) {}

Status LocalBlockReader::ReadAt(off_t offset, size_t length, char* buffer) {
  return fs_->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    Status status;
    status = posix->LSeek(fd_, offset, SEEK_SET);
    if (status.ok()) {
      status = posix->Read(fd_, buffer, length);
    }
    return status;
  });
}

void LocalBlockReader::Close() {
  fs_->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    posix->Close(fd_);
    return Status::OK();
  });
}

class AioClosure : public Closure {
 public:
  AioClosure(int inflight) : inflight_(inflight) {}

  void Run() override {
    std::unique_lock<bthread::Mutex> lk(mtx_);
    if (--inflight_ == 0) {
      cond_.notify_one();
    }
  }

  void Wait() {
    std::unique_lock<bthread::Mutex> lk(mtx_);
    while (inflight_ > 0) {
      cond_.wait(lk);
    }
  }

 private:
  int inflight_;
  bthread::Mutex mtx_;
  bthread::ConditionVariable cond_;
};

RemoteBlockReader::RemoteBlockReader(int fd, size_t blksize,
                                     std::shared_ptr<LocalFileSystem> fs,
                                     std::shared_ptr<AioQueue> aio_queue)
    : fd_(fd), blksize_(blksize), fs_(fs), aio_queue_(aio_queue) {}

Status RemoteBlockReader::ReadAt(off_t offset, size_t length, char* buffer) {
  int n = (length + blksize_ - 1) / blksize_;
  AioClosure done(n);
  std::vector<Aio*> aios;

  // prepare io
  while (length > 0) {
    size_t nbytes = std::min(length, blksize_);
    Aio* aio = new Aio(AioType::kRead, fd_, offset, nbytes, buffer, &done);
    aios.emplace_back(aio);

    length -= nbytes;
    offset += nbytes;
    buffer += nbytes;
  }

  // submit io
  CHECK_EQ(aios.size(), n);
  for (auto* aio : aios) {
    aio_queue_->Submit(aio);
  }

  // wait io
  done.Wait();

  // free aio
  for (auto* aio : aios) {
    delete aio;
  }

  return done.GetStatus();
}

void RemoteBlockReader::Close() {
  fs_->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    posix->Close(fd_);
    return Status::OK();
  });
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
