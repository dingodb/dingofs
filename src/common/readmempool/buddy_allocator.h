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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_BUDDY_ALLOCATOR_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_BUDDY_ALLOCATOR_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

namespace dingofs {

// ---- order constants (4K..64M, order 0..14) ----
inline constexpr int kMinOrderShift = 12;                       // 4K = order 0
inline constexpr size_t kMinOrderBytes = 1ULL << kMinOrderShift;  // 4096
inline constexpr int kNumOrders = 15;                            // order 0..14
inline constexpr int kMaxOrder = kNumOrders - 1;                 // 14
inline constexpr size_t kMaxOrderBytes = kMinOrderBytes << kMaxOrder;  // 64MiB

inline size_t OrderBytes(int order) { return kMinOrderBytes << order; }

// Smallest order with OrderBytes(order) >= len; returns -1 if len>64M
// (oversize, the caller handles it).
int BytesToOrder(size_t len);

// Buddy allocator: split/coalesce power-of-two blocks over a pre-allocated,
// contiguous arena.
// - Any handed-out block lives in the same arena VA span, is contiguous and
//   >=4K-aligned.
// - Memory flows freely between classes (split pushes down / coalesce merges up).
// - Guarded by a global mutex (slow path); P2 layers a TLS cache on top as the
//   lock-free fast path.
//
// order_tag_ serves two purposes (see docs/designs/dingofs-read-mempool-impl.md
// section 4.4): one byte per 4K min-block = (allocated ? 0x80 : 0) | order.
//   - high bit clear + value o = "a whole free order-o block starts here" ->
//     coalesce checks `tag==o`.
//   - high bit set = allocated (low 7 bits = order) -> OrderAt reverse-lookup.
class BuddyAllocator {
 public:
  static constexpr uint64_t kInvalid = ~0ULL;

  // base must be >=4K-aligned; total must be a multiple of kMaxOrderBytes (64M).
  BuddyAllocator(uint8_t* base, size_t total);

  BuddyAllocator(const BuddyAllocator&) = delete;
  BuddyAllocator& operator=(const BuddyAllocator&) = delete;

  // Returns an arena offset (slot address = base + off); kInvalid when exhausted.
  uint64_t Allocate(int order);
  void Free(uint64_t off, int order);

  // Order of an allocated block (for ReleaseExternal reverse-lookup). Valid only
  // for the start off of an allocated block; the tag is stable while a single
  // owner holds the block, so no lock is needed.
  int OrderAt(uint64_t off) const {
    return order_tag_[off >> kMinOrderShift] & 0x7F;
  }

  // Highest order whose free list is non-empty = the largest contiguous block
  // currently allocatable (external-fragmentation gauge). Returns -1 if empty.
  int LargestFreeOrder() const;

  // ---- debug / test ----
  size_t UsedBytes() const;          // allocated bytes (= sum of OrderBytes over handed-out blocks)
  size_t CountFree(int order) const;  // number of free blocks on the order list (O(list length))

 private:
  static constexpr uint8_t kAllocFlag = 0x80;

  uint64_t& NextOf(uint64_t off) {
    return *reinterpret_cast<uint64_t*>(base_ + off);
  }
  void PushFreeLocked(int order, uint64_t off);
  uint64_t PopFreeLocked(int order);
  void RemoveFreeLocked(int order, uint64_t off);

  uint8_t* base_;
  size_t total_;
  uint64_t free_head_[kNumOrders];
  std::vector<uint8_t> order_tag_;  // size = total / 4K
  size_t used_ = 0;
  mutable std::mutex mu_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_BUDDY_ALLOCATOR_H_
