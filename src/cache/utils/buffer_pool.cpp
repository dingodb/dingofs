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

#include "cache/utils/buffer_pool.h"

#include <butil/memory/aligned_memory.h>
#include <glog/logging.h>

namespace dingofs {
namespace cache {

BufferPool::BufferPool(size_t size, size_t alignment, size_t blksize)
    : blksize_(blksize) {
  CHECK_EQ(size % blksize, 0)
      << "Buffer pool size must be a multiple of block size";

  mem_start_ = (char*)butil::AlignedAlloc(size, alignment);
  CHECK(mem_start_ != nullptr) << "Alloc aligned memory: size = " << size
                               << ", alignment = " << alignment;

  for (int i = 0; i < size / blksize; ++i) {
    free_list_.push(i);
  }
}

BufferPool::~BufferPool() { butil::AlignedFree(mem_start_); }

char* BufferPool::Alloc() {
  std::lock_guard<BthreadMutex> lk(mutex_);
  CHECK_GE(free_list_.size(), 0);
  auto blkindex = free_list_.front();
  free_list_.pop();
  return mem_start_ + (static_cast<uint64_t>(blksize_) * blkindex);
};

void BufferPool::Free(const char* ptr) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  free_list_.push(Index(ptr));
};

int BufferPool::Index(const char* ptr) const {
  return (ptr - mem_start_) / blksize_;
}

std::vector<iovec> BufferPool::Fetch() const {
  iovec iov;
  std::vector<iovec> iovecs;
  for (const auto& index : free_list_) {
    iov.iov_base = mem_start_ + (static_cast<uint64_t>(blksize_) * index);
    iov.iov_len = blksize_;
    iovecs.push_back(iov);
  }
  return iovecs;
}

}  // namespace cache
}  // namespace dingofs
