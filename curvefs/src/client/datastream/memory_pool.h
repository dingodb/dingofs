/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_DATASTREAM_MEMORY_POOL_H_
#define CURVEFS_SRC_CLIENT_DATASTREAM_MEMORY_POOL_H_

#include <sys/types.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace curvefs {
namespace client {
namespace datastream {

// Based on article from Ben Kenwright "Fast Efficient Fixed-Size Memory Pool":
// https://arxiv.org/pdf/2210.16471
class MemoryPool {
 public:
  MemoryPool();

  bool CreatePool(size_t size_each_block, uint64_t num_total_blocks);

  void DestroyPool();

  void* Allocate();

  void DeAllocate(void* block);

  uint64_t GetFreeBlocks() const;

 private:
  void InitOneBlock();

  void AddFront(void* block);

  void RemoveFront();

  char* AddrFromIndex(uint64_t index) const;

  uint64_t IndexFromAddr(const char* addr) const;

 private:
  uint64_t size_each_block_;         // size of each block
  uint64_t num_total_blocks_;        // num of blocks
  uint64_t num_free_blocks_;         // num of remaining blocks
  uint64_t num_initialized_blocks_;  // num of initialized blocks
  char* mem_start_;                  // beginning of memory pool
  char* next_free_index_;            // num of next free block
};

}  // namespace datastream
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DATASTREAM_MEMORY_POOL_H_
