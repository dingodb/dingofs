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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_READ_MEM_POOL_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_READ_MEM_POOL_H_

#include <bvar/reducer.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "common/readmempool/arena.h"
#include "common/readmempool/buddy_allocator.h"
#include "common/readmempool/read_buf.h"
#include "common/readmempool/slab_allocator.h"
#include "common/readmempool/tls_cache.h"

namespace dingofs {

// Read-path memory pool: exposes only Allocate(len) -> contiguous slot.
// Routing: len <= kSlabMax -> slab (fixed regions, compresses internal frag);
// else -> buddy (try the TLS fast path first, miss takes buddy's global lock).
// Pre-allocated and held, never grows; returns an empty ReadBuf when exhausted.
//
// !! Lifetime: the deleter captures a raw this; a slot is returned only after the
// reply, via IOBuf refcnt. So ReadMemPool must be held by VFSHub (a mount-level
// singleton) whose lifetime is a superset of every IOBuffer carrying its slots.
// See docs/designs/dingofs-read-mempool-impl.md section 7.
class ReadMemPool {
 public:
  explicit ReadMemPool(size_t budget_bytes);

  ReadMemPool(const ReadMemPool&) = delete;
  ReadMemPool& operator=(const ReadMemPool&) = delete;

  bool Valid() const { return arena_ != nullptr; }

  // Returns a contiguous slot with cap>=len, >=4K-aligned; empty ReadBuf on
  // exhaustion/oversize.
  ReadBuf Allocate(size_t len);

  // External return entry (public, for the IOBuf deleter; frees by address
  // reverse-lookup).
  void ReleaseExternal(void* p);

  // Ready-made deleter for IOBuf::AppendUserData (callers don't hand-write a lambda).
  std::function<void(void*)> Deleter() {
    return [this](void* p) { ReleaseExternal(p); };
  }

  // Reclaim all idle memory (TLS cache + slab's retained empty blocks) to buddy.
  // Also invoked internally by Allocate before it gives up (tier-2 reclaim).
  void DrainIdle() {
    size_t before = BuddyUsedBytes();
    if (tls_) tls_->DrainAll();
    if (slab_) slab_->DrainEmpty();
    drain_reclaimed_bytes_ << static_cast<int64_t>(before - BuddyUsedBytes());
  }

  // RDMA-ready (getters only at this stage, no ibv_reg_mr call).
  uint8_t* BaseAddr() const { return arena_ ? arena_->Base() : nullptr; }

  size_t TotalSize() const { return arena_ ? arena_->Total() : 0; }

  // ---- metrics getters (read by PassiveStatus scrape callbacks) ----
  // Bytes the buddy layer has carved out of the arena. This is the aggregate:
  // it already INCLUDES the 2MB blocks owned by slab and the slots cached in
  // TLS. NOT the caller's usage (see OutstandingBytes). Component identity:
  //   BuddyUsedBytes = buddy-direct + SlabOccupiedBytes + TlsCachedBytes
  size_t BuddyUsedBytes() const { return buddy_ ? buddy_->UsedBytes() : 0; }

  // Largest allocatable order (external-fragmentation signal); -1 when empty
  // or when there is no pool.
  int LargestFreeOrder() const { return buddy_ ? buddy_->LargestFreeOrder() : -1; }

  // Slab component: total 2MB blocks slab owns / regions handed out / bytes in
  // hysteresis-retained empty blocks. SlabOccupied >= SlabUsed + SlabRetained;
  // the gap is free regions inside partially-used blocks.
  size_t SlabOccupiedBytes() const { return slab_ ? slab_->OccupiedBytes() : 0; }

  size_t SlabUsedBytes() const { return slab_ ? slab_->UsedBytes() : 0; }

  size_t SlabRetainedEmptyBytes() const {
    return slab_ ? slab_->RetainedEmptyBytes() : 0;
  }

  // TLS component: bytes currently parked in per-thread caches (carved from
  // buddy, freed by callers, reusable; reclaimed by DrainIdle).
  size_t TlsCachedBytes() const { return tls_ ? tls_->CachedBytes() : 0; }

  bool HasHugePage() const { return arena_ && arena_->HasHugePage(); }

  // ---- metrics counter getters (hot-path bvar::Adder is sharded/lock-free;
  // these merge the shards on read) ----
  // Allocate call count / count of calls that failed to return a buffer
  // (oversize, or genuinely exhausted after the tier-2 reclaim-retry).
  int64_t AllocNum() const { return alloc_num_.get_value(); }

  int64_t AllocFailNum() const { return alloc_fail_num_.get_value(); }

  // Bytes the caller currently still holds and has not returned (release-lag
  // signal).
  int64_t OutstandingBytes() const { return outstanding_bytes_.get_value(); }

  // Usage ratio for backpressure = outstanding / total. This is the demand the
  // callers actually hold, NOT the arena-carved footprint (BuddyUsedBytes),
  // since TLS cache and retained-empty slab blocks are reusable headroom. 0
  // when there is no pool.
  double UsageRatio() const {
    size_t total = TotalSize();
    return total ? static_cast<double>(OutstandingBytes()) /
                       static_cast<double>(total)
                 : 0.0;
  }

  // Cumulative requested bytes / cumulative served bytes (internal frag:
  // 1 - requested/served).
  int64_t RequestedBytes() const { return requested_bytes_.get_value(); }

  int64_t ServedBytes() const { return served_bytes_.get_value(); }

  // Cumulative bytes reclaimed by DrainIdle.
  int64_t DrainReclaimedBytes() const { return drain_reclaimed_bytes_.get_value(); }

  // TLS fast-path hit / miss counts.
  // Note: the outstanding high-water mark is NOT kept in-process (to avoid
  // merging shards on the hot path); derive it monitoring-side via
  // max_over_time(outstanding).
  int64_t TlsHitCount() const { return tls_ ? tls_->HitCount() : 0; }

  int64_t TlsMissCount() const { return tls_ ? tls_->MissCount() : 0; }

  // debug / test.
  BuddyAllocator* buddy_for_test() { return buddy_.get(); }

  SlabAllocator* slab_for_test() { return slab_.get(); }

 private:
  friend class ReadBuf;
  enum Source : uint8_t { kBuddy = 0, kSlab = 1 };

  static uint32_t MakeMeta(Source s, int order) {
    return (static_cast<uint32_t>(s) << 7) | static_cast<uint32_t>(order);
  }

  void ReturnSlot(uint64_t off, uint32_t meta);  // called by the ReadBuf dtor
  void FreeBuddy(uint64_t off, int order);       // TLS first; buddy only when full/contended

  // Account a successful Allocate (bvar::Adder shard accumulation, no
  // contention).
  void OnAllocated(size_t len, size_t cap) {
    requested_bytes_ << static_cast<int64_t>(len);
    served_bytes_ << static_cast<int64_t>(cap);
    outstanding_bytes_ << static_cast<int64_t>(cap);
  }

  // Account a free.
  void OnFreed(size_t cap) {
    outstanding_bytes_ << -static_cast<int64_t>(cap);
  }

  std::unique_ptr<Arena> arena_;
  std::unique_ptr<BuddyAllocator> buddy_;
  std::unique_ptr<SlabAllocator> slab_;
  std::unique_ptr<TlsCacheSet> tls_;

  bvar::Adder<int64_t> alloc_num_;
  bvar::Adder<int64_t> alloc_fail_num_;
  bvar::Adder<int64_t> outstanding_bytes_;
  bvar::Adder<int64_t> requested_bytes_;
  bvar::Adder<int64_t> served_bytes_;
  bvar::Adder<int64_t> drain_reclaimed_bytes_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_READ_MEM_POOL_H_
