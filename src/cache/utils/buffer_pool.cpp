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
 * Created Date: 2025-07-26
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/buffer_pool.h"

#include <butil/memory/aligned_memory.h>
#include <glog/logging.h>

#include <mutex>

namespace dingofs {
namespace cache {

BufferPool::BufferPool(size_t size, size_t alignment, size_t blksize)
    : blksize_(blksize) {
  CHECK_EQ(size % blksize, 0)
      << "Buffer pool size must be a multiple of block size";

  mem_start_ = (char*)butil::AlignedAlloc(size, alignment);
  CHECK(mem_start_ != nullptr) << "Alloc aligned memory: size = " << size
                               << ", alignment = " << alignment;

  iovec iov;
  auto& iovecs = iovecs_;
  iovecs.reserve(size / blksize);
  for (int i = 0; i < size / blksize; ++i) {
    iov.iov_base = mem_start_ + (static_cast<uint64_t>(blksize_) * i);
    iov.iov_len = blksize_;
    iovecs.push_back(iov);
    freelist_.push(i);
  }
}

BufferPool::~BufferPool() { butil::AlignedFree(mem_start_); }

char* BufferPool::Alloc() {
  std::unique_lock<BthreadMutex> lk(mutex_);
  while (freelist_.empty()) {
    can_allocate_.wait(lk);
  }

  CHECK_GE(freelist_.size(), 0);
  auto blkindex = freelist_.front();
  freelist_.pop();
  return (char*)iovecs_[blkindex].iov_base;
};

void BufferPool::Free(const char* ptr) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  freelist_.push(Index(ptr));
  can_allocate_.notify_one();
};

int BufferPool::Index(const char* ptr) const {
  return (ptr - mem_start_) / blksize_;
}

std::vector<iovec> BufferPool::RawBuffer() const { return iovecs_; }

}  // namespace cache
}  // namespace dingofs
