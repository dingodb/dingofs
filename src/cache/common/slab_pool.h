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
 * Created Date: 2026-05-26
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_SLAB_POOL_H_
#define DINGOFS_SRC_CACHE_COMMON_SLAB_POOL_H_

#include <bits/types/struct_iovec.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "common/io_buffer.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {

struct SlabBuffer {
  void* data{nullptr};
  int64_t index{-1};
  uint32_t meta{0};  // lkey
};

class SlabPool {
 public:
  SlabPool();
  ~SlabPool();

  void Init(size_t buffer_count);
  void SetMeta(uint32_t meta);
  SlabBuffer* Alloc(size_t size);
  void Free(SlabBuffer* buffer);
  int IndexOf(void* data);
  int IndexOf(SlabBuffer* buffer);
  bool Contains(void* data);
  std::vector<iovec> Fetch();

  char* Base();
  size_t BufferSize();
  size_t BufferCount();
  size_t ByteSize();

 private:
  void EnsureInitialized();

  std::mutex mutex_;
  MemoryPoolUPtr memory_pool_;
  std::vector<SlabBuffer> buffers_;
  uint32_t meta_{0};
};

void InitializeGlobalSlabPool();
void InitializeGlobalSlabPool(size_t buffer_count);
SlabPool& GetGlobalSendSlabPool();
SlabPool& GetGlobalRecvSlabPool();

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_SLAB_POOL_H_
