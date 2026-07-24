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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_TLS_CACHE_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_TLS_CACHE_H_

#include <bvar/reducer.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "common/readmempool/buddy_allocator.h"

namespace dingofs {

// Concurrent fast-path cache for buddy's small/mid classes. Modeled on
// cache/common/memory_pool's per-thread Cache, but **without
// thread_local**: a fixed
// array of cache slots (caches_[kNumCaches]); a thread maps to one slot via a
// thread-local slot id (threads beyond kNumCaches share, guarded by an
// atomic_flag try-lock). This avoids thread_local destruction issues -- the
// caches live in the pool and are reused across threads.
//
// Cache scope (see docs/designs section 6):
//   - cap is inverse to block size: cap(order)*OrderBytes(order) <= kTlsBudget;
//   - large classes (OrderBytes > kTlsBudget) get cap=0, uncached (big, rare,
//     go straight to buddy);
//   - slab's small classes do not pass here (they use slab per-bin), so this
//     only covers buddy's small/mid classes.
//
// A cached slot still looks "allocated" from buddy's view; DrainAll() returns
// them all to buddy (used for test conservation / ShrinkMem).
class TlsCacheSet {
 public:
  explicit TlsCacheSet(BuddyAllocator* buddy) : buddy_(buddy) {}

  TlsCacheSet(const TlsCacheSet&) = delete;
  TlsCacheSet& operator=(const TlsCacheSet&) = delete;

  // On hit returns a cached slot off; on miss/contention returns kInvalid
  // (caller goes to buddy).
  uint64_t TryAlloc(int order);
  // Returns true if cached; false on full/contention/large class (caller frees
  // to buddy).
  bool TryFree(int order, uint64_t off);
  // Return all cached slots to buddy (call when quiescent: test / ShrinkMem).
  void DrainAll();

  static int CapFor(int order);  // cache cap for this order (0 = uncached)

  // metrics: cumulative TryAlloc hit / miss (read-only, for the facade getter).
  int64_t HitCount() const { return hit_.get_value(); }

  int64_t MissCount() const { return miss_.get_value(); }

  // Bytes currently parked across all per-thread caches (= sum of
  // size[order]*OrderBytes(order)). Briefly locks each cache; call at scrape
  // frequency, not on the hot path.
  size_t CachedBytes() const;

 private:
  static constexpr int kNumCaches = 64;
  static constexpr int kMaxLane = 8;
  static constexpr size_t kTlsBudget =
      1ULL * 1024 * 1024;  // per-slot per-class budget: 1M

  static uint32_t Slot();  // thread-local slot id

  struct alignas(64) Cache {
    mutable std::atomic_flag lock = ATOMIC_FLAG_INIT;
    uint32_t size[kNumOrders] = {};
    uint64_t slots[kNumOrders][kMaxLane];
  };

  Cache caches_[kNumCaches];
  BuddyAllocator* buddy_;
  bvar::Adder<int64_t> hit_;
  bvar::Adder<int64_t> miss_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_TLS_CACHE_H_
