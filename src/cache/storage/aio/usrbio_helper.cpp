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
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/aio/usrbio_helper.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <sys/epoll.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <queue>

namespace dingofs {
namespace cache {
namespace storage {

IOVBuffer::IOVBuffer(hf3fs::iov* iov) : mem_start_((char*)iov->base){};

void IOVBuffer::Init(uint32_t blksize, uint32_t blocks) {
  blksize_ = blksize;
  for (int i = 0; i < blocks; i++) {
    queue_.push(i);
  }
}

char* IOVBuffer::GetBlock() {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  CHECK_GE(queue_.size(), 0);
  int blkindex = queue_.front();
  queue_.pop();
  return mem_start_ + (blksize_ * blkindex);
};

void IOVBuffer::ReleaseBlock(const char* mem) {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  int blkindex = (mem - mem_start_) / blksize_;
  queue_.push(blkindex);
};

Status Openfiles::Open(int fd, OpenFunc open_func) {
  WriteLockGuard lk(rwlock_);
  auto iter = files_.find(fd);
  if (iter == files_.end()) {
    files_.insert({fd, 1});
    return open_func(fd);
  }
  iter->second++;
  return Status::OK();
}

void Openfiles::Close(int fd, CloseFunc close_func) {
  WriteLockGuard lk(rwlock_);
  auto iter = files_.find(fd);
  if (iter != files_.end()) {
    CHECK_GE(iter->second, 0);
    if (--iter->second == 0) {
      files_.erase(iter);
      close_func(fd);
    }
  }
}

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
