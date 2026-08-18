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

#ifndef DINGOFS_CACHE_V2_CORE_MEMORY_SLAB_POOL_H_
#define DINGOFS_CACHE_V2_CORE_MEMORY_SLAB_POOL_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace dingofs {
namespace cache {
namespace v2 {

class SlabPool {
 public:
  static constexpr int kMinShift = 12;    // log2 of the smallest class
  static constexpr int kSuperShift = 22;  // log2 of a superblock
  static constexpr size_t kMinAllocSize = size_t{1} << kMinShift;
  static constexpr size_t kSuperblockSize = size_t{1} << kSuperShift;

  struct Option {
    size_t superblock_count = 64;  // 256 MiB
    int numa_node = -1;
  };

  struct Stats {
    uint64_t allocs = 0;
    uint64_t frees = 0;
    uint64_t failed_allocs = 0;
  };

  explicit SlabPool(const Option& option);
  ~SlabPool();

  SlabPool(const SlabPool&) = delete;
  SlabPool& operator=(const SlabPool&) = delete;

  char* Alloc(size_t n);
  void Free(char* p);

  // Bytes an Alloc(n) really owns. Classes are powers of two no smaller than
  // 4 KiB, so this is always at least n rounded up to a 4 KiB boundary --
  // which is what lets a block be padded out to the device's granularity
  // without a second buffer.
  static constexpr size_t SizeOf(size_t n) {
    return ObjectSizeFor(ShiftFor(n));
  }

  int SuperblockIndexOf(const char* p) const {
    auto offset = static_cast<size_t>(p - base_);  // wraps when p < base_
    return offset < total_ ? static_cast<int>(offset >> kSuperShift) : -1;
  }

  char* base() const { return base_; }
  size_t total_bytes() const { return total_; }
  size_t superblock_count() const { return metas_.size(); }
  const Stats& stats() const { return stats_; }

 private:
  static constexpr int kFreeTag = 0xFF;  // shift tag: superblock unassigned
  static constexpr int kNumSubClasses = kSuperShift - kMinShift;  // 4K..2M
  static constexpr uint16_t kNilIndex = 0xFFFF;
  static constexpr size_t kBitmapWords =
      kSuperblockSize / kMinAllocSize / 64;  // 1024 bits -> 16 words

  struct SlabMeta {
    uint8_t shift = kFreeTag;
    uint8_t cursor = 0;
    uint16_t free_count = 0;
    uint16_t prev = kNilIndex;
    uint16_t next = kNilIndex;
    uint64_t bitmap[kBitmapWords] = {};
  };

  struct SizeClass {
    uint16_t partial = kNilIndex;
    uint16_t empty_count = 0;
  };

  static constexpr int CeilLog2(size_t x) {
    return 64 - __builtin_clzll(x - 1);
  }

  static constexpr int ShiftFor(size_t n) {
    return n <= kMinAllocSize ? kMinShift : CeilLog2(n);
  }

  static constexpr size_t ObjectSizeFor(int shift) {
    return size_t{1} << shift;
  }

  static constexpr uint32_t ObjectCountFor(int shift) {
    return 1u << (kSuperShift - shift);
  }

  static constexpr uint32_t BitmapWordsFor(int shift) {
    return (ObjectCountFor(shift) + 63) / 64;
  }

  // Bitmap primitives: bit i tracks object i, 1 = free.
  static constexpr bool TestBit(const uint64_t* bitmap, uint32_t i) {
    return (bitmap[i / 64] >> (i % 64)) & 1;
  }

  static constexpr void SetBit(uint64_t* bitmap, uint32_t i) {
    bitmap[i / 64] |= uint64_t{1} << (i % 64);
  }

  static constexpr void ClearBit(uint64_t* bitmap, uint32_t i) {
    bitmap[i / 64] &= ~(uint64_t{1} << (i % 64));
  }

  // Sets bits [0, n), leaving the rest of the touched words zero.
  static constexpr void SetFirstBits(uint64_t* bitmap, uint32_t n) {
    for (uint32_t w = 0; w < n / 64; ++w) {
      bitmap[w] = ~uint64_t{0};
    }
    if (n % 64 != 0) {
      bitmap[n / 64] = (uint64_t{1} << (n % 64)) - 1;
    }
  }

  char* SuperblockAddress(uint32_t sb) const {
    return base_ + (size_t{sb} << kSuperShift);
  }

  uint32_t PopFreeSuperblock();  // kNilIndex when none left anywhere
  void PushFreeSuperblock(uint32_t sb);
  uint32_t ReclaimCachedEmpty();  // steal a class's cached empty superblock
  void DetachFromClass(SizeClass& size_class, SlabMeta& meta, int shift);
  char* AllocSuperblock();
  char* AllocFromClass(int shift);
  void FreeToClass(uint32_t sb, size_t offset, int shift);
  void ListPushFront(SizeClass& size_class, uint32_t sb);
  void ListRemove(SizeClass& size_class, SlabMeta& meta);
  void LogExhausted(size_t size);

  char* base_ = nullptr;
  size_t total_ = 0;
  uint16_t free_head_ = kNilIndex;  // free-superblock list through metas_.next
  std::array<SizeClass, kNumSubClasses> classes_;
  std::vector<SlabMeta> metas_;  // one entry per superblock
  Stats stats_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_MEMORY_SLAB_POOL_H_
