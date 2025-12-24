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

#ifndef DINGOFS_SRC_CACHE_IUTIL_BUFFER_POOL_H_
#define DINGOFS_SRC_CACHE_IUTIL_BUFFER_POOL_H_

#include <bits/types/struct_iovec.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/memory/aligned_memory.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <queue>

namespace dingofs {
namespace cache {

// TODO: shared bufferpool if lock contention is high
class BufferPool {
 public:
  BufferPool(size_t buffer_size, size_t buffer_count, size_t alignment)
      : buffer_size_(buffer_size) {
    size_t total_size = buffer_size * buffer_count;
    mem_start_ = (char*)butil::AlignedAlloc(total_size, alignment);

    CHECK(mem_start_ != nullptr)
        << "Fail to alloc aligned memory{size=" << total_size
        << " alignment=" << alignment << "}";

    iovec iov;
    iovecs_.reserve(buffer_count);
    for (size_t i = 0; i < buffer_count; ++i) {
      iov.iov_base = mem_start_ + (buffer_size_ * i);
      iov.iov_len = buffer_size_;
      iovecs_.push_back(iov);
      freeindexs_.push(i);
    }

    LOG(INFO) << "Successfully create BufferPool{buffer_size=" << buffer_size_
              << " buffer_count=" << buffer_count << " alignment=" << alignment
              << "}";
  }

  ~BufferPool() { butil::AlignedFree(mem_start_); }

  char* Alloc() {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    while (freeindexs_.empty()) {
      can_allocate_.wait(lock);
    }
    int index = freeindexs_.front();
    freeindexs_.pop();
    return static_cast<char*>(iovecs_[index].iov_base);
  }

  void Free(const char* ptr) {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    freeindexs_.push(Index(ptr));
    can_allocate_.notify_one();
  }

  int Index(const char* ptr) const { return (ptr - mem_start_) / buffer_size_; }
  std::vector<iovec> Fetch() const { return iovecs_; }

 private:
  char* mem_start_;
  size_t buffer_size_;
  std::queue<int> freeindexs_;  // free buffer index
  std::vector<iovec> iovecs_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable can_allocate_;
};

using BufferPoolUPtr = std::unique_ptr<BufferPool>;
using BufferPoolSPtr = std::shared_ptr<BufferPool>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_BUFFER_POOL_H_
