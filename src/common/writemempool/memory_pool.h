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
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_COMMON_WRITEMEMPOOL_MEMORY_POOL_H_
#define DINGOFS_SRC_COMMON_WRITEMEMPOOL_MEMORY_POOL_H_

#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace dingofs {

class MemoryPool;
using MemoryPoolUPtr = std::unique_ptr<MemoryPool>;

class MemoryPool {
 public:
  MemoryPool(char* base, size_t buffer_size, size_t buffer_count);
  ~MemoryPool();
  static MemoryPoolUPtr Create(size_t buffer_size, size_t buffer_count);

  char* Require();
  void Release(char* buffer);

  // RDMA-ready: the pool is one contiguous region (single mmap), so these give
  // ibv_reg_mr its addr + length. Mirrors ReadMemPool's BaseAddr()/TotalSize().
  char* BaseAddr() const { return base_; }
  size_t BufferSize() const { return buffer_size_; }
  size_t BufferCount() const { return buffer_count_; }
  size_t TotalSize() const { return buffer_size_ * buffer_count_; }

  size_t IndexOf(char* buffer) const {
    DCHECK_GE(buffer, base_);
    DCHECK_LT(buffer, base_ + (buffer_size_ * buffer_count_));
    return (buffer - base_) / buffer_size_;
  }

 private:
  static constexpr uint32_t kNumShards = 32;
  static constexpr uint32_t kNumCaches = 128;
  static constexpr uint32_t kCacheCap = 8;
  static constexpr uint32_t kRefillBatch = 4;
  static constexpr uint32_t kNil = 0xFFFFFFFFu;

  struct alignas(64) Shard {
    std::atomic<uint64_t> head;
  };

  struct alignas(64) Cache {
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    uint32_t size = 0;
    uint32_t entries[kCacheCap];
  };

  static void* HugePagesMalloc(size_t size);
  static void HugePagesFree(void* ptr);
  static uint32_t ThreadSlot();

  static uint64_t Pack(uint32_t idx, uint32_t ver) {
    return (static_cast<uint64_t>(idx) << 32) | ver;
  }
  static uint32_t Idx(uint64_t h) { return static_cast<uint32_t>(h >> 32); }
  static uint32_t Ver(uint64_t h) { return static_cast<uint32_t>(h); }

  uint32_t& NextOf(uint32_t idx) {
    return *reinterpret_cast<uint32_t*>(base_ + (idx * buffer_size_));
  }

  uint32_t TryPopFromShard(Shard& shard);
  char* TryRequireFromShard(Shard& shard);
  void RefillCacheFromShards(Cache& c, uint32_t start_shard);
  void FlushCacheToShards(Cache& c, uint32_t flush_count, uint32_t home_shard);

  char* base_;
  size_t buffer_size_;
  size_t buffer_count_;
  Shard shards_[kNumShards];
  Cache caches_[kNumCaches];
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_MEMORY_POOL_H_
