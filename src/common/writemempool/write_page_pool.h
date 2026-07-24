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

#ifndef DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_PAGE_POOL_H_
#define DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_PAGE_POOL_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

namespace dingofs {

class WritePagePool;
using WritePagePoolUPtr = std::unique_ptr<WritePagePool>;

class WritePagePool {
 public:
  static WritePagePoolUPtr Create(size_t page_size, size_t page_count);
  ~WritePagePool();

  // Acquires up to `count` pages and stores them in pages[0..return_value).
  // A short result is allowed; only the returned prefix becomes outstanding.
  size_t RequireBatch(char** pages, size_t count);

  // Returns exactly `count` outstanding pages to this pool. Every entry must
  // have been returned by this pool, entries must be pairwise distinct, and
  // none may have been released already. Ownership of all entries transfers
  // back to the pool when this function is called. Violating this exactly-once
  // contract corrupts the intrusive free lists; production builds do not scan
  // the pool to detect duplicate or stale releases.
  void ReleaseBatch(char* const* pages, size_t count);

  // RDMA-ready arena geometry. The complete tuple is needed both to register
  // the MR and to map an address back to its fixed-size page index.
  char* BaseAddr() const { return base_; }
  size_t BufferSize() const { return page_size_; }
  size_t BufferCount() const { return page_count_; }
  size_t TotalSize() const { return page_size_ * page_count_; }

 private:
  friend class WritePagePoolTestPeer;

  static constexpr uint32_t kNumShards = 32;
  static constexpr uint32_t kNil = UINT32_MAX;
  static constexpr uint32_t kCacheCapacity = 32;
  static constexpr uint32_t kOwnerExtentPages = kCacheCapacity;

  struct alignas(64) Shard {
    mutable std::mutex mutex;
    uint32_t head{kNil};
    size_t count{0};
  };

  struct alignas(64) Cache {
    mutable std::mutex mutex;
    uint32_t size{0};
    uint32_t next_refill_shard{0};
    std::array<uint32_t, kCacheCapacity> entries{};
  };

  struct Chain {
    uint32_t first{kNil};
    uint32_t last{kNil};
    size_t count{0};
  };

  WritePagePool(char* base, size_t page_size, size_t page_count);

  uint32_t& NextOf(uint32_t index) { return next_[index]; }
  // page_shift_ is 0 when page_size_ is not a power of two (the shift fast
  // path only applies to the power-of-two geometry used in production).
  uint32_t IndexOf(const char* page) const {
    const size_t offset = page - base_;
    return static_cast<uint32_t>(page_shift_ != 0 ? offset >> page_shift_
                                                  : offset / page_size_);
  }
  // Test-only. Call only while pool operations are quiescent; this is not a
  // coherent snapshot under concurrent allocation and release.
  size_t TEST_FreePages() const;
  bool PageAligned(size_t offset) const {
    return page_shift_ != 0 ? (offset & (page_size_ - 1)) == 0
                            : offset % page_size_ == 0;
  }
  uint32_t OwnerShard(const char* page) const {
    return OwnerOfIndex(IndexOf(page));
  }
  static uint32_t OwnerOfIndex(uint32_t index) {
    return (index / kOwnerExtentPages) % kNumShards;
  }
  char* PageAt(uint32_t index) const { return base_ + (index * page_size_); }

  static void* AllocateArena(size_t size);
  static void FreeArena(void* ptr);

  bool RefillCache(Cache* cache);
  size_t TakeFromCache(Cache* cache, char** pages, size_t count);
  size_t RequireBatchFromCache(char** pages, size_t count,
                               uint32_t cache_index);

  char* base_;
  size_t page_size_;
  size_t page_shift_;
  size_t page_count_;
  std::unique_ptr<uint32_t[]> next_;
  std::array<Shard, kNumShards> shards_;
  uint32_t cache_count_{0};
  std::unique_ptr<Cache[]> caches_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_PAGE_POOL_H_
