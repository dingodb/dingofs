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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_BASE_BUFFER_POOL_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_BASE_BUFFER_POOL_H_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/slab_pool.h"
#include "blockcache/infiniband/base/memory_registry.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class BufferPool {
 public:
  explicit BufferPool(const SlabPoolOption& option)
      : pool_(std::make_unique<SlabPool>(option)) {}

  BufferPool(const BufferPool&) = delete;
  BufferPool& operator=(const BufferPool&) = delete;

  Status Init(MemoryRegistry* registry) {
    StatusOr<const MemoryRegion*> mr =
        registry->Register(pool_->base(), pool_->total_bytes());
    if (!mr.ok()) {
      return mr.status();
    }

    lkey_ = mr.value()->lkey();
    rkey_ = mr.value()->rkey();
    return Status::OK();
  }

  char* Alloc(size_t n) { return pool_->Alloc(n); }
  void Free(char* p) { pool_->Free(p); }

  uint32_t lkey() const { return lkey_; }
  uint32_t rkey() const { return rkey_; }
  char* base() const { return pool_->base(); }
  size_t total_bytes() const { return pool_->total_bytes(); }

 private:
  SlabPoolUPtr pool_;
  uint32_t lkey_ = 0;
  uint32_t rkey_ = 0;
};

using BufferPoolUPtr = std::unique_ptr<BufferPool>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_BASE_BUFFER_POOL_H_
