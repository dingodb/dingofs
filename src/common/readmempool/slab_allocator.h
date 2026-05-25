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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_SLAB_ALLOCATOR_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_SLAB_ALLOCATOR_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "common/readmempool/buddy_allocator.h"

namespace dingofs {

// <=kSlabMax goes to slab (fixed regions, compresses buddy's 2^k internal frag);
// otherwise goes to buddy.
inline constexpr size_t kSlabMax = 64 * 1024;          // 64K
inline constexpr int kNumSlabClasses = 5;              // 4K/8K/16K/32K/64K
inline constexpr size_t kSlabBlockBytes = 2ULL * 1024 * 1024;  // fixed 2MB slab block
inline constexpr int kSlabBlockOrder = 9;              // 2MB = 4K<<9
inline constexpr int kSlabBlockShift = 21;             // log2(2MB)

// Small-class allocator: take a **fixed 2MB block** from buddy and carve it into
// fixed-size regions; a fully-empty slab returns its 2MB block to buddy
// (cross-class flow). The fixed block size makes "region off -> owning slab" an
// O(1) mask+array lookup (see docs/designs/dingofs-read-mempool-impl.md section 5).
//
// P3: guarded by a single mutex (including buddy take/return); lock order is
// fixed slab->buddy, so there is no deadlock.
class SlabAllocator {
 public:
  static constexpr uint64_t kInvalid = ~0ULL;
  static constexpr uint32_t kRegNil = ~0u;

  SlabAllocator(BuddyAllocator* buddy, uint8_t* base, size_t total);

  ~SlabAllocator();

  SlabAllocator(const SlabAllocator&) = delete;

  SlabAllocator& operator=(const SlabAllocator&) = delete;

  // len <= kSlabMax. Returns the region's arena offset + actual capacity;
  // kInvalid when exhausted.
  uint64_t Allocate(size_t len, size_t* out_cap);

  // region off -> owning slab -> return region. When a slab becomes fully empty,
  // **retain one empty slab** (hysteresis, avoids small-class alloc-then-free
  // thrashing of 2MB blocks); only the 2nd empty slab returns its block to buddy.
  // Returns the freed region's byte size (so the facade can decrement outstanding).
  size_t Free(uint64_t off);

  // Whether off falls inside a slab block (lets ReadMemPool decide the source).
  bool OwnsBlock(uint64_t off) const;

  // Return every bin's retained empty slab block to buddy (call before ShrinkMem
  // / test conservation).
  void DrainEmpty();

  // Bytes occupied by slab = (number of 2MB blocks slab owns) * 2MB. Superset of
  // UsedBytes + RetainedEmptyBytes (the gap = free regions in partial blocks).
  size_t OccupiedBytes() const;

  // Bytes handed out via slab = sum over slabs of (total-free)*region_bytes.
  size_t UsedBytes() const;

  // Bytes held by hysteresis-retained empty slabs = sum of empty_count*2MB.
  size_t RetainedEmptyBytes() const;

  // debug / test.
  size_t UsedRegions() const;

  static int BinIndex(size_t len);  // 0..4; returns -1 if len>kSlabMax

 private:
  struct Slab {
    uint64_t block_off;     // 2MB block start (2MB-aligned)
    uint32_t region_bytes;
    uint32_t total;         // total region count
    uint32_t free_cnt;
    uint32_t free_head;     // intrusive free-list head (region index; kRegNil = empty)
    Slab* prev = nullptr;   // partial doubly-linked list
    Slab* next = nullptr;
  };

  struct Bin {
    uint32_t region_bytes = 0;
    Slab* partial = nullptr;  // list of slabs with free regions (incl. the retained empty slab)
    int empty_count = 0;       // number of fully-empty slabs in this bin (hysteresis keeps <=1)
  };

  // The first 4 bytes of a region store the "next free region index" (intrusive list).
  uint32_t& RegionNext(uint64_t region_off) {
    return *reinterpret_cast<uint32_t*>(base_ + region_off);
  }

  void PushPartial(Bin& bin, Slab* s);

  void RemovePartial(Bin& bin, Slab* s);

  BuddyAllocator* buddy_;
  uint8_t* base_;
  Bin bins_[kNumSlabClasses];
  std::vector<Slab*> slab_owner_;  // idx = off>>kSlabBlockShift -> Slab* (null if not a slab block)
  size_t used_regions_ = 0;
  mutable std::mutex mu_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_SLAB_ALLOCATOR_H_
