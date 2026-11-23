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

#include "blockcache/core/memory/buffer.h"

#include <glog/logging.h>

#include <memory>

#include "blockcache/core/memory/shard_allocator.h"

namespace dingofs {
namespace blockcache {

// One per shard, created on the shard thread so its pages are NUMA-local.
static thread_local std::unique_ptr<SlabPool> tls_pool;

Buffer Buffer::Alloc(size_t n) {
  SlabPool* pool = tls_pool.get();
  if (pool == nullptr || n == 0) {
    return {};
  }
  char* data = pool->Alloc(n);
  if (data == nullptr) {
    // No heap fallback on purpose: the device rejects unregistered memory.
    return {};
  }
  return Buffer(data, n);
}

void Buffer::Reset() noexcept {
  if (base_ != nullptr) {
    SlabPool* pool = tls_pool.get();
    DCHECK(pool != nullptr) << "a Buffer outlived its shard's pool";
    if (pool != nullptr) {
      pool->Free(base_);  // PopFront moved data_; the pool knows only this one
    }
    base_ = nullptr;
    data_ = nullptr;
    size_ = 0;
  }
}

Status BufferPool::Open(size_t superblocks) {
  if (superblocks == 0) {
    return Status::InvalidParam(
        "buffer pool must hold at least one 4MiB superblock");
  }
  if (tls_pool != nullptr) {
    return Status::OK();
  }
  SlabPoolOption option;
  option.superblock_count = superblocks;
  option.numa_node = memory::LocalNumaNode();
  tls_pool = std::make_unique<SlabPool>(option);
  return Status::OK();
}

void BufferPool::Close() { tls_pool.reset(); }

SlabPool* BufferPool::LocalPool() { return tls_pool.get(); }

}  // namespace blockcache
}  // namespace dingofs
