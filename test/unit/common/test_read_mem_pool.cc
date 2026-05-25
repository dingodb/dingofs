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

#include "common/readmempool/read_mem_pool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "common/readmempool/buddy_allocator.h"
#include "common/readmempool/slab_allocator.h"

namespace dingofs {

namespace {
constexpr size_t kMiB = 1024 * 1024;
constexpr size_t kKiB = 1024;
bool Is4kAligned(const void* p) {
  return (reinterpret_cast<uintptr_t>(p) & (4096 - 1)) == 0;
}
}  // namespace

// ---------------- functional ----------------

TEST(ReadMemPoolTest, CreateAndGetters) {
  ReadMemPool pool(128 * kMiB);
  ASSERT_TRUE(pool.Valid());
  EXPECT_NE(pool.BaseAddr(), nullptr);
  EXPECT_TRUE(Is4kAligned(pool.BaseAddr()));
  // 2MB-aligned
  EXPECT_EQ(reinterpret_cast<uintptr_t>(pool.BaseAddr()) % kArenaAlign, 0u);
  // total is rounded up to a multiple of 64M
  EXPECT_EQ(pool.TotalSize() % kMaxOrderBytes, 0u);
  EXPECT_GE(pool.TotalSize(), 128 * kMiB);
}

TEST(ReadMemPoolTest, AllocContiguousAlignedCapacity) {
  ReadMemPool pool(128 * kMiB);
  ASSERT_TRUE(pool.Valid());

  // large class 64M
  ReadBuf big = pool.Allocate(64 * kMiB);
  ASSERT_TRUE(static_cast<bool>(big));
  EXPECT_TRUE(Is4kAligned(big.data()));
  EXPECT_GE(big.capacity(), 64u * kMiB);
  // contiguous and writable (write one byte at each end, no crash)
  big.data()[0] = 1;
  big.data()[64 * kMiB - 1] = 2;

  // small class 5K -> rounded up to 8K (order 1)
  ReadBuf small = pool.Allocate(5 * kKiB);
  ASSERT_TRUE(static_cast<bool>(small));
  EXPECT_TRUE(Is4kAligned(small.data()));
  EXPECT_GE(small.capacity(), 5u * kKiB);
  EXPECT_EQ(small.capacity(), 8u * kKiB);  // 2^ceil(log2 5K)=8K

  // the two blocks do not overlap
  uint8_t* a = big.data();
  uint8_t* b = small.data();
  bool disjoint = (a + big.capacity() <= b) || (b + small.capacity() <= a);
  EXPECT_TRUE(disjoint);
}

TEST(ReadMemPoolTest, AllAllocationsAre4kAligned) {
  ReadMemPool pool(128 * kMiB);
  std::vector<ReadBuf> bufs;
  for (size_t len : {1u * kKiB, 4u * kKiB, 7u * kKiB, 100u * kKiB, 1u * kMiB,
                     3u * kMiB, 16u * kMiB}) {
    ReadBuf b = pool.Allocate(len);
    ASSERT_TRUE(static_cast<bool>(b)) << "len=" << len;
    EXPECT_TRUE(Is4kAligned(b.data())) << "len=" << len;
    EXPECT_GE(b.capacity(), len);
    bufs.push_back(std::move(b));
  }
}

TEST(ReadMemPoolTest, ExhaustionReturnsEmptyThenRecovers) {
  ReadMemPool pool(64 * kMiB);  // a single 64M top block
  ASSERT_TRUE(pool.Valid());

  ReadBuf whole = pool.Allocate(64 * kMiB);
  ASSERT_TRUE(static_cast<bool>(whole));

  // full: another allocation should return an empty handle, no crash
  ReadBuf none = pool.Allocate(4 * kKiB);
  EXPECT_FALSE(static_cast<bool>(none));

  // recovers after release
  whole = ReadBuf();  // dtor returns it
  ReadBuf again = pool.Allocate(4 * kKiB);
  EXPECT_TRUE(static_cast<bool>(again));
}

// Core: after random alloc/free then releasing all, buddy returns to its initial
// state (conservation + coalesce correctness).
TEST(ReadMemPoolTest, SpaceConservationAfterRandomAllocFree) {
  const size_t budget = 128 * kMiB;
  ReadMemPool pool(budget);
  ASSERT_TRUE(pool.Valid());
  BuddyAllocator* buddy = pool.buddy_for_test();

  const size_t top_blocks = pool.TotalSize() / kMaxOrderBytes;
  ASSERT_EQ(buddy->UsedBytes(), 0u);
  ASSERT_EQ(buddy->CountFree(kMaxOrder), top_blocks);

  std::mt19937 rng(12345);
  std::uniform_int_distribution<size_t> size_dist(1, 4 * kMiB);
  std::vector<ReadBuf> live;

  for (int iter = 0; iter < 20000; ++iter) {
    bool do_alloc = live.empty() || (rng() & 1);
    if (do_alloc) {
      ReadBuf b = pool.Allocate(size_dist(rng));
      if (b) live.push_back(std::move(b));  // skip if full
    } else {
      size_t idx = rng() % live.size();
      std::swap(live[idx], live.back());
      live.pop_back();  // dtor returns it
    }
  }
  // release everything
  live.clear();
  pool.DrainIdle();  // reclaim TLS cache + slab's retained empty blocks

  // back to initial: used==0, top-block count restored, other order lists empty
  EXPECT_EQ(buddy->UsedBytes(), 0u);
  EXPECT_EQ(buddy->CountFree(kMaxOrder), top_blocks);
  for (int o = 0; o < kMaxOrder; ++o) {
    EXPECT_EQ(buddy->CountFree(o), 0u) << "order " << o << " list should be empty";
  }
}

TEST(ReadMemPoolTest, SplitThenCoalesceBackToTop) {
  ReadMemPool pool(64 * kMiB);  // 1 top block
  BuddyAllocator* buddy = pool.buddy_for_test();
  ASSERT_EQ(buddy->CountFree(kMaxOrder), 1u);

  // split the 64M top block into 16 x 4M
  std::vector<ReadBuf> bufs;
  for (int i = 0; i < 16; ++i) {
    ReadBuf b = pool.Allocate(4 * kMiB);
    ASSERT_TRUE(static_cast<bool>(b)) << "i=" << i;
    bufs.push_back(std::move(b));
  }
  EXPECT_EQ(buddy->CountFree(kMaxOrder), 0u);  // the top block is fully carved
  EXPECT_EQ(buddy->UsedBytes(), 16u * 4 * kMiB);

  // release all -> coalesce merges back into 1 x 64M top block
  bufs.clear();
  EXPECT_EQ(buddy->UsedBytes(), 0u);
  EXPECT_EQ(buddy->CountFree(kMaxOrder), 1u);
}

TEST(ReadMemPoolTest, DisownTransfersOwnership) {
  ReadMemPool pool(64 * kMiB);
  BuddyAllocator* buddy = pool.buddy_for_test();

  // use 4M (order 10, > TLS budget, not cached in TLS) -> ReleaseExternal frees
  // straight to buddy.
  ReadBuf b = pool.Allocate(4 * kMiB);
  ASSERT_TRUE(static_cast<bool>(b));
  EXPECT_GT(buddy->UsedBytes(), 0u);

  void* raw = b.Disown();          // hand off ownership
  EXPECT_FALSE(static_cast<bool>(b));
  b = ReadBuf();                  // b dtor is a no-op, will not return
  EXPECT_GT(buddy->UsedBytes(), 0u);  // still allocated (owner transferred outside)

  pool.ReleaseExternal(raw);        // external return (address reverse-lookup -> buddy)
  EXPECT_EQ(buddy->UsedBytes(), 0u);
}

// ---------------- P3 slab ----------------

TEST(ReadMemPoolTest, SlabRoutingAndOwnership) {
  ReadMemPool pool(128 * kMiB);
  SlabAllocator* slab = pool.slab_for_test();

  // small class (<=64K) -> slab; 3K rounds up to the 4K class
  ReadBuf s = pool.Allocate(3 * kKiB);
  ASSERT_TRUE(static_cast<bool>(s));
  EXPECT_EQ(s.capacity(), 4u * kKiB);
  EXPECT_TRUE(Is4kAligned(s.data()));
  uint64_t soff = static_cast<uint64_t>(s.data() - pool.BaseAddr());
  EXPECT_TRUE(slab->OwnsBlock(soff));

  // large class (>64K) -> buddy; OwnsBlock false
  ReadBuf big = pool.Allocate(1 * kMiB);
  ASSERT_TRUE(static_cast<bool>(big));
  uint64_t boff = static_cast<uint64_t>(big.data() - pool.BaseAddr());
  EXPECT_FALSE(slab->OwnsBlock(boff));
}

TEST(ReadMemPoolTest, SlabHysteresisRetainsOneEmptyThenDrains) {
  ReadMemPool pool(64 * kMiB);
  SlabAllocator* slab = pool.slab_for_test();
  BuddyAllocator* buddy = pool.buddy_for_test();
  const size_t top = pool.TotalSize() / kMaxOrderBytes;

  std::vector<ReadBuf> v;
  for (int i = 0; i < 600; ++i) {  // 600x4K spans multiple 2MB slab blocks (512 regions/block)
    ReadBuf b = pool.Allocate(4 * kKiB);
    ASSERT_TRUE(static_cast<bool>(b));
    v.push_back(std::move(b));
  }
  EXPECT_GT(buddy->UsedBytes(), 0u);

  // release all: every region returned, but hysteresis retains 1 empty slab
  // block (not returned to buddy immediately).
  v.clear();
  EXPECT_EQ(slab->UsedRegions(), 0u);
  EXPECT_EQ(buddy->UsedBytes(), kSlabBlockBytes);  // the 4K bin retains one 2MB empty block

  // hysteresis reuse: another 4K alloc/free should not add buddy usage (reuses
  // the retained empty block).
  { ReadBuf b = pool.Allocate(4 * kKiB); ASSERT_TRUE(static_cast<bool>(b)); }
  EXPECT_EQ(buddy->UsedBytes(), kSlabBlockBytes);  // still only 1 block, buddy untouched

  // DrainIdle reclaims the retained empty block -> top block restored.
  pool.DrainIdle();
  EXPECT_EQ(buddy->UsedBytes(), 0u);
  EXPECT_EQ(buddy->CountFree(kMaxOrder), top);
}

TEST(ReadMemPoolTest, MixedSlabBuddyConservation) {
  ReadMemPool pool(128 * kMiB);
  BuddyAllocator* buddy = pool.buddy_for_test();
  SlabAllocator* slab = pool.slab_for_test();
  const size_t top = pool.TotalSize() / kMaxOrderBytes;

  std::mt19937 rng(7);
  std::uniform_int_distribution<size_t> d(1, 8 * kMiB);  // spans the slab/buddy boundary
  std::vector<ReadBuf> live;
  for (int i = 0; i < 30000; ++i) {
    if (live.empty() || (rng() & 1)) {
      ReadBuf b = pool.Allocate(d(rng));
      if (b) live.push_back(std::move(b));
    } else {
      size_t k = rng() % live.size();
      std::swap(live[k], live.back());
      live.pop_back();
    }
  }
  live.clear();
  pool.DrainIdle();
  EXPECT_EQ(slab->UsedRegions(), 0u);
  EXPECT_EQ(buddy->UsedBytes(), 0u);
  EXPECT_EQ(buddy->CountFree(kMaxOrder), top);
}

// ---------------- P2 TLS ----------------

TEST(ReadMemPoolTest, TlsCachesMidSizeAndDrains) {
  ReadMemPool pool(64 * kMiB);
  BuddyAllocator* buddy = pool.buddy_for_test();

  // 256K = order 6 (TLS cap>0): after release it's cached in TLS, buddy still
  // counts it as used.
  { ReadBuf b = pool.Allocate(256 * kKiB); ASSERT_TRUE(static_cast<bool>(b)); }
  EXPECT_GT(buddy->UsedBytes(), 0u);  // cached in TLS, not returned to buddy
  EXPECT_EQ(pool.TlsCachedBytes(), 256u * kKiB);  // parked in TLS

  pool.DrainIdle();
  EXPECT_EQ(buddy->UsedBytes(), 0u);
  EXPECT_EQ(pool.TlsCachedBytes(), 0u);  // drained back to buddy

  ReadBuf b2 = pool.Allocate(256 * kKiB);  // reuse (may hit TLS)
  EXPECT_TRUE(static_cast<bool>(b2));
}

TEST(ReadMemPoolTest, MultiThreadConservation) {
  ReadMemPool pool(256 * kMiB);
  BuddyAllocator* buddy = pool.buddy_for_test();
  SlabAllocator* slab = pool.slab_for_test();
  const size_t top = pool.TotalSize() / kMaxOrderBytes;

  const int kThreads = 8;
  const int kPerThread = 20000;
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&pool, t, kPerThread]() {
      std::mt19937 rng(1000 + t);
      std::uniform_int_distribution<size_t> d(1, 2 * kMiB);
      std::vector<ReadBuf> live;
      for (int i = 0; i < kPerThread; ++i) {
        if (live.empty() || (rng() & 1)) {
          ReadBuf b = pool.Allocate(d(rng));
          if (b) live.push_back(std::move(b));
        } else {
          size_t k = rng() % live.size();
          std::swap(live[k], live.back());
          live.pop_back();
        }
      }
    });  // live dtor returns the slots
  }
  for (auto& th : ts) th.join();
  pool.DrainIdle();
  EXPECT_EQ(slab->UsedRegions(), 0u);
  EXPECT_EQ(buddy->UsedBytes(), 0u);
  EXPECT_EQ(buddy->CountFree(kMaxOrder), top);
}

// ---------------- metrics getters (Phase-1 step 1.1) ----------------

TEST(ReadMemPoolTest, MetricsGettersUsedSlab) {
  ReadMemPool pool(128 * kMiB);
  ASSERT_TRUE(pool.Valid());

  EXPECT_EQ(pool.BuddyUsedBytes(), 0u);
  EXPECT_EQ(pool.SlabUsedBytes(), 0u);
  EXPECT_EQ(pool.SlabRetainedEmptyBytes(), 0u);

  ReadBuf big = pool.Allocate(4 * kMiB);  // buddy (order 10)
  ASSERT_TRUE(static_cast<bool>(big));
  EXPECT_EQ(pool.BuddyUsedBytes(), 4u * kMiB);
  EXPECT_EQ(pool.SlabUsedBytes(), 0u);

  ReadBuf small = pool.Allocate(4 * kKiB);  // slab: takes one 2MB block from buddy
  ASSERT_TRUE(static_cast<bool>(small));
  EXPECT_EQ(pool.SlabUsedBytes(), 4u * kKiB);
  EXPECT_EQ(pool.SlabOccupiedBytes(), kSlabBlockBytes);  // one 2MB block
  EXPECT_EQ(pool.BuddyUsedBytes(), 4u * kMiB + kSlabBlockBytes);

  big = ReadBuf();    // free 4M (order 10 bypasses TLS, returns straight to buddy)
  small = ReadBuf();  // free 4K -> slab empty -> hysteresis retains 1 empty 2MB block
  EXPECT_EQ(pool.SlabUsedBytes(), 0u);
  EXPECT_EQ(pool.SlabRetainedEmptyBytes(), kSlabBlockBytes);
  EXPECT_EQ(pool.SlabOccupiedBytes(), kSlabBlockBytes);  // still owns the empty block
  EXPECT_EQ(pool.BuddyUsedBytes(), kSlabBlockBytes);  // only the retained slab block left

  pool.DrainIdle();
  EXPECT_EQ(pool.BuddyUsedBytes(), 0u);
  EXPECT_EQ(pool.SlabRetainedEmptyBytes(), 0u);
  EXPECT_EQ(pool.SlabOccupiedBytes(), 0u);
}

TEST(ReadMemPoolTest, LargestFreeOrderTracksFragmentation) {
  ReadMemPool pool(64 * kMiB);  // one 64M top block
  ASSERT_TRUE(pool.Valid());
  EXPECT_EQ(pool.LargestFreeOrder(), kMaxOrder);  // a whole 64M is free

  ReadBuf b = pool.Allocate(4 * kMiB);  // split the 64M -> no whole 64M block left
  ASSERT_TRUE(static_cast<bool>(b));
  EXPECT_LT(pool.LargestFreeOrder(), kMaxOrder);
  EXPECT_GE(pool.LargestFreeOrder(), 0);

  b = ReadBuf();
  pool.DrainIdle();
  EXPECT_EQ(pool.LargestFreeOrder(), kMaxOrder);  // coalesced back to a whole 64M
}

// ---------------- metrics counters (Phase-1 step 1.2) ----------------

TEST(ReadMemPoolTest, MetricsCounters) {
  ReadMemPool pool(64 * kMiB);
  ASSERT_TRUE(pool.Valid());
  EXPECT_EQ(pool.AllocNum(), 0);
  EXPECT_EQ(pool.OutstandingBytes(), 0);

  {
    ReadBuf a = pool.Allocate(4 * kMiB);  // buddy, cap 4M
    ReadBuf b = pool.Allocate(4 * kKiB);  // slab, cap 4K
    ASSERT_TRUE(static_cast<bool>(a));
    ASSERT_TRUE(static_cast<bool>(b));
    EXPECT_EQ(pool.AllocNum(), 2);
    EXPECT_EQ(pool.OutstandingBytes(),
              static_cast<int64_t>(4 * kMiB + 4 * kKiB));
  }
  EXPECT_EQ(pool.OutstandingBytes(), 0);  // all freed -> outstanding back to zero

  // Cumulative internal frag: request 5K -> serve 8K, so served > requested.
  {
    ReadBuf c = pool.Allocate(5 * kKiB);
    ASSERT_TRUE(static_cast<bool>(c));
    EXPECT_EQ(c.capacity(), 8u * kKiB);
  }
  EXPECT_GT(pool.ServedBytes(), pool.RequestedBytes());

  pool.DrainIdle();  // reclaim the slab's retained empty block
  EXPECT_GE(pool.DrainReclaimedBytes(), static_cast<int64_t>(kSlabBlockBytes));

  // Fail count: after drain a whole 64M is allocatable; the next allocation
  // then fills the pool -> empty handle + alloc_fail++.
  ReadBuf whole = pool.Allocate(64 * kMiB);
  ASSERT_TRUE(static_cast<bool>(whole));
  int64_t fails_before = pool.AllocFailNum();
  ReadBuf none = pool.Allocate(4 * kKiB);
  EXPECT_FALSE(static_cast<bool>(none));
  EXPECT_EQ(pool.AllocFailNum(), fails_before + 1);
}

TEST(ReadMemPoolTest, MetricsTlsHitMiss) {
  ReadMemPool pool(64 * kMiB);
  ASSERT_TRUE(pool.Valid());
  EXPECT_EQ(pool.TlsHitCount(), 0);

  // First 256K (order 6, TLS-cacheable): TLS empty -> miss -> buddy.
  { ReadBuf b = pool.Allocate(256 * kKiB); ASSERT_TRUE(static_cast<bool>(b)); }
  EXPECT_GE(pool.TlsMissCount(), 1);  // freed slot now goes into TLS

  // Second 256K: hits TLS.
  { ReadBuf b = pool.Allocate(256 * kKiB); ASSERT_TRUE(static_cast<bool>(b)); }
  EXPECT_GE(pool.TlsHitCount(), 1);
}

TEST(ReadMemPoolTest, UsageRatioReflectsOutstanding) {
  ReadMemPool pool(64 * kMiB);
  ASSERT_TRUE(pool.Valid());
  EXPECT_DOUBLE_EQ(pool.UsageRatio(), 0.0);

  {
    ReadBuf b = pool.Allocate(32 * kMiB);  // cap 32M = half the pool
    ASSERT_TRUE(static_cast<bool>(b));
    EXPECT_NEAR(pool.UsageRatio(), 0.5, 1e-9);  // outstanding 32M / total 64M
  }
  EXPECT_DOUBLE_EQ(pool.UsageRatio(), 0.0);  // freed -> back to zero
}

// tier-2: TLS-cached idle slots fragment the arena; a whole-pool alloc fails
// in buddy but the reclaim-and-retry in Allocate drains them and succeeds.
TEST(ReadMemPoolTest, ReclaimRetryRescuesLargeAlloc) {
  ReadMemPool pool(64 * kMiB);
  ASSERT_TRUE(pool.Valid());

  // Carve 4x 256K from the single 64M top block, then free all at once: they
  // land in this thread's TLS cache (cap 4 for order 6), so buddy still counts
  // them carved -> no contiguous 64M remains.
  {
    ReadBuf bufs[4];
    for (auto& b : bufs) {
      b = pool.Allocate(256 * kKiB);
      ASSERT_TRUE(static_cast<bool>(b));
    }
  }
  EXPECT_LT(pool.LargestFreeOrder(), kMaxOrder);  // arena fragmented by TLS

  // Without reclaim this 64M alloc fails; tier-2 DrainIdle returns the cached
  // slots, coalesces, and the retry succeeds.
  int64_t reclaimed_before = pool.DrainReclaimedBytes();
  ReadBuf whole = pool.Allocate(64 * kMiB);
  EXPECT_TRUE(static_cast<bool>(whole));
  EXPECT_GT(pool.DrainReclaimedBytes(), reclaimed_before);  // reclaim fired
}

// Per-component breakdown reconciles:
//   BuddyUsedBytes (carved) = buddy-direct + SlabOccupied + TlsCached
TEST(ReadMemPoolTest, ComponentBreakdownReconciles) {
  ReadMemPool pool(64 * kMiB);
  ASSERT_TRUE(pool.Valid());

  ReadBuf big = pool.Allocate(8 * kMiB);  // buddy-direct, cap 8M (held)
  ReadBuf s1 = pool.Allocate(4 * kKiB);   // slab bin 0 -> one 2MB block
  ReadBuf s2 = pool.Allocate(8 * kKiB);   // slab bin 1 -> another 2MB block
  ASSERT_TRUE(big && s1 && s2);
  { ReadBuf mid = pool.Allocate(256 * kKiB); }  // freed -> cached in TLS

  const size_t kBuddyDirect = 8u * kMiB;
  EXPECT_EQ(pool.SlabOccupiedBytes(), 2u * kSlabBlockBytes);  // two bins, two blocks
  EXPECT_EQ(pool.TlsCachedBytes(), 256u * kKiB);

  // identity: everything buddy carved = held large + slab blocks + TLS cached
  EXPECT_EQ(pool.BuddyUsedBytes(),
            kBuddyDirect + pool.SlabOccupiedBytes() + pool.TlsCachedBytes());

  // slab occupied is a superset of handed-out regions + retained-empty
  EXPECT_GE(pool.SlabOccupiedBytes(),
            pool.SlabUsedBytes() + pool.SlabRetainedEmptyBytes());
  EXPECT_EQ(pool.SlabUsedBytes(), 4u * kKiB + 8u * kKiB);
}

// ---------------- perf (baseline, single-thread) ----------------

TEST(ReadMemPoolPerf, AllocFreeThroughput1M) {
  ReadMemPool pool(256 * kMiB);
  ASSERT_TRUE(pool.Valid());

  const int kIters = 200000;
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < kIters; ++i) {
    ReadBuf b = pool.Allocate(1 * kMiB);  // alloc then immediately free (reuses the same class)
    ASSERT_TRUE(static_cast<bool>(b));
  }
  auto t1 = std::chrono::steady_clock::now();
  double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
  double ns_per_op = us * 1000.0 / kIters;  // one alloc+free
  LOG(INFO) << "[perf] 1M alloc+free: " << kIters << " ops, "
            << ns_per_op << " ns/op, "
            << (kIters / (us / 1e6)) << " ops/s";
  SUCCEED();
}

TEST(ReadMemPoolPerf, MixedSizeThroughput) {
  ReadMemPool pool(256 * kMiB);
  ASSERT_TRUE(pool.Valid());
  const size_t sizes[] = {4 * kKiB, 64 * kKiB, 256 * kKiB, 1 * kMiB, 4 * kMiB};

  const int kIters = 200000;
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < kIters; ++i) {
    ReadBuf b = pool.Allocate(sizes[i % 5]);
    ASSERT_TRUE(static_cast<bool>(b));
  }
  auto t1 = std::chrono::steady_clock::now();
  double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
  LOG(INFO) << "[perf] mixed alloc+free: " << kIters << " ops, "
            << (us * 1000.0 / kIters) << " ns/op";
  SUCCEED();
}

// Multi-thread: 256K (order 6, hits the TLS fast path) to show TLS's concurrent
// throughput gain.
TEST(ReadMemPoolPerf, MultiThreadThroughput256K) {
  ReadMemPool pool(512 * kMiB);
  ASSERT_TRUE(pool.Valid());
  const int kThreads = 8;
  const int kPerThread = 200000;
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&]() {
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire)) {
      }
      for (int i = 0; i < kPerThread; ++i) {
        ReadBuf b = pool.Allocate(256 * kKiB);  // alloc then immediately free
      }
    });
  }
  while (ready.load() < kThreads) {
  }
  auto t0 = std::chrono::steady_clock::now();
  go.store(true, std::memory_order_release);
  for (auto& th : ts) th.join();
  auto t1 = std::chrono::steady_clock::now();
  double s = std::chrono::duration<double>(t1 - t0).count();
  long total = static_cast<long>(kThreads) * kPerThread;
  LOG(INFO) << "[perf] MT 256K alloc+free: " << kThreads << " threads, " << total
            << " ops, " << (total / s) << " ops/s";
  pool.DrainIdle();
  SUCCEED();
}

}  // namespace dingofs
