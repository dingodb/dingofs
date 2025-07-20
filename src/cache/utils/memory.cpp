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
 * Created Date: 2025-07-18
 * Author: Jingli Chen (Wine93)
 */

#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>

namespace dingofs {
namespace cache {

// 分配大页内存，size 必须为大页大小的整数倍
void* AllocHugePage(size_t size) {
  void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (ptr == MAP_FAILED) {
    throw std::runtime_error(std::string("mmap hugepage failed: ") +
                             std::strerror(errno));
  }
  return ptr;
}

// 释放大页内存
void FreeHugePage(void* ptr, size_t size) {
  if (munmap(ptr, size) != 0) {
    throw std::runtime_error(std::string("munmap hugepage failed: ") +
                             std::strerror(errno));
  }
}

}  // namespace cache
}  // namespace dingofs
