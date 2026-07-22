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
 * Created Date: 2026-07-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_SLAB_ALLOCATOR_H_
#define DINGOFS_SRC_CACHE_COMMON_SLAB_ALLOCATOR_H_

#include <bthread/mutex.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/const.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {

class SlabAllocator;
using SlabAllocatorUPtr = std::unique_ptr<SlabAllocator>;

class SlabAllocator {
 public:
  static constexpr size_t kMinObjectSize = 4 * kKiB;
  static constexpr size_t kSuperblockSize = 4 * kMiB;

  // superblock_count in (0, 0xFFFF).
  static SlabAllocatorUPtr Create(size_t superblock_count);
  ~SlabAllocator();

  // Returns nullptr when size is 0, > 4 MiB, or the pool is exhausted;
  // never blocks.
  char* Alloc(size_t size);

  // Invalid frees (out-of-pool, misaligned, double free) fail a CHECK in
  // every build type; Free(nullptr) is a no-op.
  void Free(char* ptr);

  // Superblock index for io_uring buf_index; -1 when outside the pool.
  int IndexOf(const char* ptr) const;

  char* BaseAddr() const { return base_; }
  size_t TotalSize() const { return total_size_; }
  size_t SuperblockCount() const { return pool_->BufferCount(); }

 private:
  static constexpr int kMinShift = 12;    // log2(kMinObjectSize)
  static constexpr int kSuperShift = 22;  // log2(kSuperblockSize)
  static constexpr int kFreeTag = 0xFF;   // tag: superblock is in the pool
  static constexpr int kNumSubClasses = kSuperShift - kMinShift;  // 4K..2M
  static constexpr uint16_t kNilIndex = 0xFFFF;
  static constexpr size_t kBitmapWords =
      kSuperblockSize / kMinObjectSize / 64;  // 1024 bits -> 16 words

  static_assert(size_t{1} << kMinShift == kMinObjectSize);
  static_assert(size_t{1} << kSuperShift == kSuperblockSize);

  // shift is atomic only for the unlocked reads (I2); all accesses relaxed.
  struct alignas(64) SlabMeta {
    std::atomic<uint8_t> shift{kFreeTag};  // kFreeTag or log2(object size)
    uint16_t free_count{0};
    uint16_t prev{kNilIndex};  // intrusive partial-list links
    uint16_t next{kNilIndex};
    uint64_t bitmap[kBitmapWords]{};  // bit i set <=> object i is free
  };

  struct alignas(64) SizeClass {
    bthread::Mutex mutex;
    uint16_t partial{kNilIndex};  // head of partial-superblock list
  };

  explicit SlabAllocator(MemoryPoolUPtr pool);

  // Smallest s with (1 << s) >= x; requires x >= 2.
  static int CeilLog2(size_t x) { return 64 - __builtin_clzll(x - 1); }

  // Bitmap primitives: bit i tracks object i, 1 = free.
  static bool TestBit(const uint64_t* bitmap, uint32_t i) {
    return (bitmap[i / 64] >> (i % 64)) & 1;
  }

  static void SetBit(uint64_t* bitmap, uint32_t i) {
    bitmap[i / 64] |= uint64_t{1} << (i % 64);
  }

  static void ClearBit(uint64_t* bitmap, uint32_t i) {
    bitmap[i / 64] &= ~(uint64_t{1} << (i % 64));
  }

  // Sets bits [0, n), leaving the rest of the touched words zero.
  static void SetFirstBits(uint64_t* bitmap, uint32_t n) {
    for (uint32_t w = 0; w < n / 64; ++w) {
      bitmap[w] = ~uint64_t{0};
    }
    if (n % 64 != 0) {
      bitmap[n / 64] = (uint64_t{1} << (n % 64)) - 1;
    }
  }

  // Index of the lowest set bit; requires word != 0.
  static uint32_t FirstSetBit(uint64_t word) { return __builtin_ctzll(word); }

  static int ShiftFor(size_t size);

  static size_t ObjectSizeFor(int shift) { return size_t{1} << shift; }

  static uint32_t ObjectCountFor(int shift) {
    return 1u << (kSuperShift - shift);
  }

  static uint32_t BitmapWordsFor(int shift) {
    return (ObjectCountFor(shift) + 63) / 64;
  }

  // Pool geometry: offset is relative to base_, ordinal is the object's
  // position inside its superblock.
  static uint32_t SuperblockOf(size_t offset) {
    return static_cast<uint32_t>(offset / kSuperblockSize);
  }

  static uint32_t OrdinalOf(size_t offset, int shift) {
    return static_cast<uint32_t>((offset % kSuperblockSize) >> shift);
  }

  static bool IsAligned(size_t offset, int shift) {
    return offset % ObjectSizeFor(shift) == 0;
  }

  char* SuperblockAddress(uint32_t sb) const {
    return base_ + (size_t{sb} * kSuperblockSize);
  }

  char* ObjectAddress(uint32_t sb, uint32_t ordinal, int shift) const {
    return SuperblockAddress(sb) + (size_t{ordinal} * ObjectSizeFor(shift));
  }

  char* AllocSuperblock();
  char* AllocFromClass(int shift);
  void FreeToClass(uint32_t sb, size_t offset, int shift);
  void AttachToClass(SizeClass& size_class, uint32_t sb, int shift);
  void DetachFromClass(SizeClass& size_class, SlabMeta& meta, int shift);
  uint32_t PopFreeObject(SlabMeta& meta, uint32_t words);
  void ListPushFront(SizeClass& size_class, uint32_t sb);
  void ListRemove(SizeClass& size_class, SlabMeta& meta);
  void LogExhausted(size_t size) const;

  MemoryPoolUPtr pool_;
  char* const base_;
  const size_t total_size_;
  std::vector<SlabMeta> meta_;  // one entry per superblock
  std::array<SizeClass, kNumSubClasses> classes_;
  mutable std::atomic<uint64_t> exhausted_count_{0};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_SLAB_ALLOCATOR_H_
