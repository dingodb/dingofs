/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-06-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_SLAB_BUFFER_H_
#define DINGOFS_SRC_CACHE_COMMON_SLAB_BUFFER_H_

#include <bits/types/struct_iovec.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {

struct SlabBuffer {
  char* data{nullptr};
  uint32_t capacity{0};
  uint32_t length{0};
  uint32_t lkey{0};
  uint32_t rkey{0};
  uint32_t index{0};
};

class SlabBufferPool;
using SlabBufferPoolUPtr = std::unique_ptr<SlabBufferPool>;

class SlabBufferPool {
 public:
  static SlabBufferPoolUPtr Create(size_t buffer_size, size_t buffer_count);

  SlabBuffer* Alloc();
  void Free(SlabBuffer* buffer);
  int IndexOf(SlabBuffer* buffer) const;
  int IndexOf(const char* data) const;
  size_t BufferCount() const { return memory_pool_->BufferCount(); }
  std::vector<iovec> Fetch() const;

  char* BaseAddr() const { return memory_pool_->BaseAddr(); }
  size_t TotalSize() const { return memory_pool_->TotalSize(); }
  void SetRdmaKeys(uint32_t lkey, uint32_t rkey);

 private:
  explicit SlabBufferPool(MemoryPoolUPtr memory_pool);

  MemoryPoolUPtr memory_pool_;
  std::vector<SlabBuffer> buffers_;
};

Status InitializeGlobalSlabPool();
SlabBufferPool* GetGlobalReadSlabPool();
SlabBufferPool* GetGlobalWriteSlabPool();
void SetGlobalSlabPools(SlabBufferPoolUPtr read, SlabBufferPoolUPtr write);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_SLAB_BUFFER_H_
