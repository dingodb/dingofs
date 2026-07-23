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
 * Created Date: 2026-07-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_SLAB_POOL_H_
#define DINGOFS_SRC_CACHE_COMMON_SLAB_POOL_H_

#include <bits/types/struct_iovec.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "cache/common/slab_allocator.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class SlabPool;

// A move-only lease over a size-class slab from a registered SlabPool. The
// destructor returns the slab unless it was handed to an IOBuffer (MoveInto),
// so every early-return path is leak-free without hand-written cleanup.
class SlabLease {
 public:
  SlabLease() = default;
  SlabLease(SlabPool* pool, char* data) : pool_(pool), data_(data) {}
  ~SlabLease() { Reset(); }

  SlabLease(SlabLease&& other) noexcept
      : pool_(other.pool_), data_(other.data_) {
    other.pool_ = nullptr;
    other.data_ = nullptr;
  }
  SlabLease& operator=(SlabLease&& other) noexcept {
    if (this != &other) {
      Reset();
      pool_ = other.pool_;
      data_ = other.data_;
      other.pool_ = nullptr;
      other.data_ = nullptr;
    }
    return *this;
  }
  SlabLease(const SlabLease&) = delete;
  SlabLease& operator=(const SlabLease&) = delete;

  bool ok() const { return data_ != nullptr; }
  char* data() const { return data_; }
  uint32_t lkey() const;
  uint32_t rkey() const;
  int index() const;  // owning superblock, for io_uring fixed buffers

  // Hands the slab to the IOBuffer refcount world: appends [data, length) with
  // the pool lkey and a deleter that returns the slab, then releases the lease.
  void MoveInto(IOBuffer* buffer, size_t length);

 private:
  void Reset();

  SlabPool* pool_{nullptr};
  char* data_{nullptr};
};

// A process-global pool of RDMA/io_uring-registered memory, sub-allocated by
// size class via SlabAllocator. One MR covers the whole pool, so lkey/rkey are
// uniform across every slab.
class SlabPool {
 public:
  static std::unique_ptr<SlabPool> Create(size_t superblock_count);

  SlabLease Acquire(size_t size);  // invalid lease when empty or size > 4 MiB
  void Free(char* data);
  int IndexOf(const char* data) const;  // superblock index, -1 if out of pool
  std::vector<iovec> Fetch() const;     // superblock iovecs for io_uring

  char* BaseAddr() const { return alloc_->BaseAddr(); }
  size_t TotalSize() const { return alloc_->TotalSize(); }
  uint32_t Lkey() const { return lkey_; }
  uint32_t Rkey() const { return rkey_; }
  void SetRdmaKeys(uint32_t lkey, uint32_t rkey) {
    lkey_ = lkey;
    rkey_ = rkey;
  }

 private:
  explicit SlabPool(SlabAllocatorUPtr alloc) : alloc_(std::move(alloc)) {}

  SlabAllocatorUPtr alloc_;
  uint32_t lkey_{0};
  uint32_t rkey_{0};
};

using SlabPoolUPtr = std::unique_ptr<SlabPool>;

Status InitializeGlobalSlabPool();
SlabPool* GetGlobalSlabPool();

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_SLAB_POOL_H_
