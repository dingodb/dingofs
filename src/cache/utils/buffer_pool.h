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

#ifndef DINGOFS_SRC_CACHE_UTILS_BUFFER_POOL_H_
#define DINGOFS_SRC_CACHE_UTILS_BUFFER_POOL_H_

#include <cstddef>
#include <memory>
#include <queue>

#include "cache/common/type.h"

namespace dingofs {
namespace cache {

class BufferPool {
 public:
  BufferPool(size_t size, size_t alignment, size_t blksize);

  ~BufferPool();

  char* Alloc();
  void Free(const char* ptr);

  int Index(const char* ptr) const;

  std::vector<iovec> RawBuffer() const;

 private:
  char* mem_start_;
  size_t blksize_;
  std::queue<int> freelist_;
  std::vector<iovec> iovecs_;
  BthreadMutex mutex_;
  BthreadConditionVariable can_allocate_;
};

using BufferPoolUPtr = std::unique_ptr<BufferPool>;
using BufferPoolSPtr = std::shared_ptr<BufferPool>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_BUFFER_POOL_H_
