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

#ifdef WITH_LIBUSRBIO

#include "cache/storage/aio/usrbio_helper.h"

namespace dingofs {
namespace cache {

BlockBufferPool::BlockBufferPool(USRBIOApi::IOV* iov, uint32_t blksize,
                                 uint32_t blocks)
    : mem_start_((char*)iov->base), blksize_(blksize) {
  for (uint32_t i = 0; i < blocks; i++) {
    free_list_.push(i);
  }
};

char* BlockBufferPool::Get() {
  std::lock_guard<BthreadMutex> lk(mutex_);
  CHECK_GE(free_list_.size(), 0);
  auto blkindex = free_list_.front();
  free_list_.pop();
  return mem_start_ + (static_cast<uint64_t>(blksize_) * blkindex);
};

void BlockBufferPool::Release(const char* mem) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  auto blkindex = (mem - mem_start_) / blksize_;
  free_list_.push(blkindex);
};

Status FdRegister::RegFd(int fd) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  auto iter = registed_fds_.find(fd);
  if (iter != registed_fds_.end()) {
    iter->second++;
    return Status::OK();
  }

  auto status = USRBIOApi::RegFd(fd);
  if (status.ok()) {
    registed_fds_.insert({fd, 1});
  }
  return status;
}

void FdRegister::DeRegFd(int fd) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  auto iter = registed_fds_.find(fd);
  if (iter == registed_fds_.end()) {
    return;
  }

  CHECK_GE(iter->second, 0);
  if (--iter->second == 0) {
    registed_fds_.erase(iter);
    USRBIOApi::DeRegFd(fd);
  }
}

}  // namespace cache
}  // namespace dingofs

#endif  // WITH_LIBUSRBIO
