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
 * Author: AI
 */

#include <bthread/bthread.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cache/common/slab_allocator.h"
#include "common/const.h"

namespace dingofs {
namespace cache {

namespace {

constexpr size_t kSuperblockSize = SlabAllocator::kSuperblockSize;  // 4 MiB

// Objects carved from a fresh superblock come out in ascending address
// order (the bitmap pops the lowest set bit first), so the distance between
// two back-to-back allocations reveals the rounded class size. Only valid for
// sub-classes: above 2 MiB the two allocations are independent MemoryPool
// buffers whose relative order is unspecified. A CHECK failure here means the
// environment could not back the pool, not a probe misuse.
size_t ProbeClassSize(size_t size) {
  CHECK_LE(size, 2 * kMiB) << "ProbeClassSize only valid for sub-classes";
  auto allocator = SlabAllocator::Create(2);
  CHECK(allocator != nullptr);
  char* first = allocator->Alloc(size);
  char* second = allocator->Alloc(size);
  CHECK(first != nullptr && second != nullptr);
  size_t distance = second - first;
  allocator->Free(second);
  allocator->Free(first);
  return distance;
}

// Power-of-two class capacity a request rounds up to, mirroring the
// allocator's ShiftFor. Lets tests stamp the last byte of the whole object.
size_t ClassCapacity(size_t size) {
  if (size <= 4 * kKiB) {
    return 4 * kKiB;
  }
  if (size > 2 * kMiB) {
    return kSuperblockSize;
  }
  size_t capacity = 4 * kKiB;
  while (capacity < size) {
    capacity *= 2;
  }
  return capacity;
}

std::string ClassLabel(size_t size) {
  if (size >= kMiB) {
    return std::to_string(size / kMiB) + "m";
  }
  return std::to_string(size / kKiB) + "k";
}

template <typename... Args>
std::string Row(const char* format, Args... args) {
  char buffer[96];
  CHECK_GE(std::snprintf(buffer, sizeof(buffer), format, args...), 0);
  return buffer;
}

// Runs `body` `ops` times and returns nanoseconds per op: the one place for the
// steady_clock + duration_cast boilerplate the benches share.
template <typename Fn>
double TimeNsPerOp(int ops, const Fn& body) {
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < ops; ++i) {
    body();
  }
  auto elapsed = std::chrono::steady_clock::now() - start;
  return static_cast<double>(
             std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed)
                 .count()) /
         ops;
}

// Runs `fn` inside one bthread and returns after it joins, so the measured
// allocation path executes in the production (bthread) calling context.
template <typename Fn>
void RunInBthread(const Fn& fn) {
  auto entry = [](void* raw) -> void* {
    (*static_cast<const Fn*>(raw))();
    return nullptr;
  };
  bthread_t tid;
  CHECK_EQ(
      bthread_start_background(&tid, nullptr, entry, const_cast<Fn*>(&fn)), 0);
  bthread_join(tid, nullptr);
}

// Single-worker latency: `rounds` alloc/free pairs of `size`, timed inside a
// bthread. Returns nanoseconds per pair.
double BenchPairNs(SlabAllocator* allocator, size_t size, int rounds) {
  double ns = 0;
  RunInBthread([&] {
    ns = TimeNsPerOp(rounds, [&] {
      char* buffer = allocator->Alloc(size);
      CHECK(buffer != nullptr);
      allocator->Free(buffer);
    });
  });
  return ns;
}

// One worker's steady-state loop: pin one object (sub-classes) so the
// superblock stays carved, then `iters` alloc/free pairs of `size`.
void AllocFreeWorker(SlabAllocator* allocator, size_t size, int iters) {
  // Under concurrency Alloc may return nullptr with capacity still free
  // (contended CAS in MemoryPool, the out-of-lock Release window); the pool is
  // sized so it is never genuinely exhausted here, so spin.
  auto alloc = [allocator, size] {
    char* buffer = allocator->Alloc(size);
    for (int spins = 0; buffer == nullptr; ++spins) {
      CHECK_LT(spins, 1000000) << "alloc stuck: pool truly exhausted";
      buffer = allocator->Alloc(size);
    }
    return buffer;
  };
  char* pin = size < SlabAllocator::kSuperblockSize ? alloc() : nullptr;
  for (int i = 0; i < iters; ++i) {
    allocator->Free(alloc());
  }
  if (pin != nullptr) {
    allocator->Free(pin);
  }
}

struct BthreadWorkerArg {
  SlabAllocator* allocator;
  size_t size;
  int iters;
};

void* BthreadWorkerEntry(void* raw) {
  auto* arg = static_cast<BthreadWorkerArg*>(raw);
  AllocFreeWorker(arg->allocator, arg->size, arg->iters);
  return nullptr;
}

// One bthread per entry in `sizes`, each running AllocFreeWorker on its size;
// returns million pairs per second across all workers. bthreads are the
// production calling context: a contended bthread::Mutex suspends the bthread
// cooperatively rather than blocking a pthread.
double BenchMtMops(SlabAllocator* allocator, const std::vector<size_t>& sizes,
                   int total_ops) {
  int iters = total_ops / static_cast<int>(sizes.size());
  std::vector<bthread_t> tids(sizes.size(), 0);
  std::vector<BthreadWorkerArg> args(sizes.size());
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < sizes.size(); ++i) {
    args[i] = {allocator, sizes[i], iters};
    CHECK_EQ(bthread_start_background(&tids[i], nullptr, BthreadWorkerEntry,
                                      &args[i]),
             0);
  }
  for (auto tid : tids) {
    bthread_join(tid, nullptr);
  }
  auto secs =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count();
  return static_cast<double>(total_ops) / secs / 1e6;
}

}  // namespace

TEST(SlabAllocatorTest, Create) {
  {  // invalid superblock counts
    EXPECT_EQ(SlabAllocator::Create(0), nullptr);
    EXPECT_EQ(SlabAllocator::Create(0xFFFF), nullptr);
  }

  {
    auto allocator = SlabAllocator::Create(8);
    ASSERT_NE(allocator, nullptr);
    EXPECT_NE(allocator->BaseAddr(), nullptr);
    EXPECT_EQ(allocator->TotalSize(), 8 * kSuperblockSize);
    EXPECT_EQ(allocator->SuperblockCount(), 8);
  }
}

TEST(SlabAllocatorTest, AllocRoundsUpToClass) {
  {  // out-of-range sizes
    auto allocator = SlabAllocator::Create(2);
    ASSERT_NE(allocator, nullptr);
    EXPECT_EQ(allocator->Alloc(0), nullptr);
    EXPECT_EQ(allocator->Alloc(kSuperblockSize + 1), nullptr);
  }

  {  // sub-classes round up to the smallest fitting power of two
    EXPECT_EQ(ProbeClassSize(1), 4 * kKiB);
    EXPECT_EQ(ProbeClassSize(4 * kKiB), 4 * kKiB);
    EXPECT_EQ(ProbeClassSize((4 * kKiB) + 1), 8 * kKiB);
    EXPECT_EQ(ProbeClassSize(12 * kKiB), 16 * kKiB);
    EXPECT_EQ(ProbeClassSize(32 * kKiB), 32 * kKiB);
    EXPECT_EQ(ProbeClassSize((32 * kKiB) + 1), 64 * kKiB);
    EXPECT_EQ(ProbeClassSize(2 * kMiB), 2 * kMiB);
  }

  {  // anything above 2 MiB takes a whole superblock
    auto allocator = SlabAllocator::Create(2);
    ASSERT_NE(allocator, nullptr);
    char* buffer = allocator->Alloc((2 * kMiB) + 1);
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ((buffer - allocator->BaseAddr()) % kSuperblockSize, 0);
    allocator->Free(buffer);
  }
}

TEST(SlabAllocatorTest, Alignment) {
  auto allocator = SlabAllocator::Create(4);
  ASSERT_NE(allocator, nullptr);

  for (size_t size = 4 * kKiB; size <= kSuperblockSize; size *= 2) {
    char* buffer = allocator->Alloc(size);
    ASSERT_NE(buffer, nullptr) << "size=" << size;
    EXPECT_EQ(reinterpret_cast<uintptr_t>(buffer) % (4 * kKiB), 0)
        << "size=" << size;
    allocator->Free(buffer);
  }
}

TEST(SlabAllocatorTest, WholeSuperblockFastPath) {
  auto allocator = SlabAllocator::Create(8);
  ASSERT_NE(allocator, nullptr);

  std::vector<char*> buffers;
  std::set<int> indexes;
  for (int i = 0; i < 8; ++i) {
    char* buffer = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(buffer, nullptr);
    buffers.push_back(buffer);
    indexes.insert(allocator->IndexOf(buffer));
  }
  EXPECT_EQ(indexes.size(), 8);  // all distinct superblocks
  EXPECT_EQ(allocator->Alloc(kSuperblockSize), nullptr);  // exhausted

  for (auto* buffer : buffers) {
    allocator->Free(buffer);
  }
  char* buffer = allocator->Alloc(kSuperblockSize);  // usable again
  ASSERT_NE(buffer, nullptr);
  allocator->Free(buffer);
}

TEST(SlabAllocatorTest, AllocFreeSingleClass) {
  auto allocator = SlabAllocator::Create(2);
  ASSERT_NE(allocator, nullptr);

  // A 4 KiB class superblock holds exactly 1024 objects.
  std::vector<char*> buffers;
  std::set<char*> distinct;
  int index = -1;
  for (int i = 0; i < 1024; ++i) {
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    if (index < 0) {
      index = allocator->IndexOf(buffer);
    }
    EXPECT_EQ(allocator->IndexOf(buffer), index);
    std::memset(buffer, i % 256, 4 * kKiB);
    buffers.push_back(buffer);
    distinct.insert(buffer);
  }
  EXPECT_EQ(distinct.size(), 1024);

  for (int i = 0; i < 1024; ++i) {  // no overlap: every pattern intact
    EXPECT_EQ(buffers[i][0], static_cast<char>(i % 256));
    EXPECT_EQ(buffers[i][(4 * kKiB) - 1], static_cast<char>(i % 256));
  }

  for (auto* buffer : buffers) {
    allocator->Free(buffer);
  }
  // The drained superblock went back to the pool: both are whole-allocatable.
  char* first = allocator->Alloc(kSuperblockSize);
  char* second = allocator->Alloc(kSuperblockSize);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  allocator->Free(first);
  allocator->Free(second);
}

TEST(SlabAllocatorTest, PartialSlabReuse) {
  auto allocator = SlabAllocator::Create(2);
  ASSERT_NE(allocator, nullptr);

  char* a = allocator->Alloc(4 * kKiB);
  char* b = allocator->Alloc(4 * kKiB);
  char* c = allocator->Alloc(4 * kKiB);
  ASSERT_NE(c, nullptr);

  allocator->Free(b);
  char* d = allocator->Alloc(4 * kKiB);
  EXPECT_EQ(d, b);  // lowest free slot of the same partial superblock

  allocator->Free(a);
  allocator->Free(c);
  allocator->Free(d);
}

TEST(SlabAllocatorTest, PartialListMiddleRemoval) {
  auto allocator = SlabAllocator::Create(4);
  ASSERT_NE(allocator, nullptr);

  // Fill three superblocks with 64 KiB objects (64 per superblock).
  constexpr size_t kSize = 64 * kKiB;
  constexpr int kPerBlock = kSuperblockSize / kSize;
  std::map<int, std::vector<char*>> blocks;
  for (int i = 0; i < 3 * kPerBlock; ++i) {
    char* buffer = allocator->Alloc(kSize);
    ASSERT_NE(buffer, nullptr);
    blocks[allocator->IndexOf(buffer)].push_back(buffer);
  }
  ASSERT_EQ(blocks.size(), 3);

  // One hole per block: each flips full -> partial and is pushed to the list
  // head, so the free order below fixes the partial list as [c, b, a].
  auto it = blocks.begin();
  auto& a = it->second;
  auto& b = (++it)->second;
  auto& c = (++it)->second;
  char* a_hole = a.back();
  char* b_hole = b.back();
  char* c_hole = c.back();
  a.pop_back();
  b.pop_back();
  c.pop_back();
  allocator->Free(a_hole);
  allocator->Free(b_hole);
  allocator->Free(c_hole);

  // Drain b entirely: it leaves the list from the middle (prev and next both
  // set) and its superblock returns to the pool.
  for (auto* buffer : b) {
    allocator->Free(buffer);
  }

  // The list must now be [c, a]: allocs hit c's hole then a's hole.
  EXPECT_EQ(allocator->Alloc(kSize), c_hole);
  EXPECT_EQ(allocator->Alloc(kSize), a_hole);

  allocator->Free(c_hole);
  allocator->Free(a_hole);
  for (auto* buffer : a) {
    allocator->Free(buffer);
  }
  for (auto* buffer : c) {
    allocator->Free(buffer);
  }
}

TEST(SlabAllocatorTest, CapacityFlowsAcrossClasses) {
  auto allocator = SlabAllocator::Create(2);
  ASSERT_NE(allocator, nullptr);

  std::map<int, std::vector<char*>> by_superblock;
  for (int i = 0; i < 2048; ++i) {  // 4 KiB objects eat the whole pool
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    by_superblock[allocator->IndexOf(buffer)].push_back(buffer);
  }
  ASSERT_EQ(by_superblock.size(), 2);
  EXPECT_EQ(allocator->Alloc(4 * kKiB), nullptr);
  EXPECT_EQ(allocator->Alloc(kSuperblockSize), nullptr);

  // Draining one superblock frees it for any other class.
  auto& [index, buffers] = *by_superblock.begin();
  for (auto* buffer : buffers) {
    allocator->Free(buffer);
  }
  char* whole = allocator->Alloc(kSuperblockSize);
  ASSERT_NE(whole, nullptr);
  EXPECT_EQ(allocator->IndexOf(whole), index);

  allocator->Free(whole);
  for (auto* buffer : by_superblock.rbegin()->second) {
    allocator->Free(buffer);
  }
}

// Reverse of the flow above: a whole 4 MiB block, freed, then carved for a
// sub-class. The whole path never touches meta.bitmap, so AttachToClass must
// see a clean slate; a regression stamping the bitmap on the whole path would
// hand out overlapping objects here.
TEST(SlabAllocatorTest, WholeThenClassReuse) {
  auto allocator = SlabAllocator::Create(1);
  ASSERT_NE(allocator, nullptr);

  char* whole = allocator->Alloc(kSuperblockSize);
  ASSERT_NE(whole, nullptr);
  allocator->Free(whole);

  std::vector<char*> buffers;
  std::set<char*> distinct;
  for (int i = 0; i < 1024; ++i) {  // the sole superblock, carved into 4 KiB
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(allocator->IndexOf(buffer), 0);
    std::memset(buffer, i % 256, 4 * kKiB);
    buffers.push_back(buffer);
    distinct.insert(buffer);
  }
  EXPECT_EQ(distinct.size(), 1024);
  for (int i = 0; i < 1024; ++i) {  // no overlap: every pattern intact
    EXPECT_EQ(buffers[i][0], static_cast<char>(i % 256));
    EXPECT_EQ(buffers[i][(4 * kKiB) - 1], static_cast<char>(i % 256));
  }

  for (auto* buffer : buffers) {
    allocator->Free(buffer);
  }
  char* again = allocator->Alloc(kSuperblockSize);  // whole-allocatable again
  ASSERT_NE(again, nullptr);
  allocator->Free(again);
}

TEST(SlabAllocatorTest, MixedClassesNoOverlap) {
  auto allocator = SlabAllocator::Create(8);
  ASSERT_NE(allocator, nullptr);

  // One buffer per class: 8 distinct classes -> exactly 8 superblocks.
  const std::vector<size_t> sizes = {4 * kKiB,   8 * kKiB, 16 * kKiB, 64 * kKiB,
                                     128 * kKiB, 1 * kMiB, 2 * kMiB,  4 * kMiB};
  struct Allocation {
    char* buffer;
    size_t capacity;
  };
  std::vector<Allocation> allocations;
  for (size_t i = 0; i < sizes.size(); ++i) {
    char* buffer = allocator->Alloc(sizes[i]);
    ASSERT_NE(buffer, nullptr) << "size=" << sizes[i];
    buffer[0] = static_cast<char>(i);
    buffer[sizes[i] - 1] = static_cast<char>(i);
    allocations.push_back({.buffer = buffer, .capacity = sizes[i]});
  }

  for (size_t i = 0; i < allocations.size(); ++i) {  // patterns intact
    EXPECT_EQ(allocations[i].buffer[0], static_cast<char>(i));
    EXPECT_EQ(allocations[i].buffer[allocations[i].capacity - 1],
              static_cast<char>(i));
  }

  for (size_t i = 0; i < allocations.size(); ++i) {  // intervals disjoint
    for (size_t j = i + 1; j < allocations.size(); ++j) {
      const auto& a = allocations[i];
      const auto& b = allocations[j];
      EXPECT_TRUE(a.buffer + a.capacity <= b.buffer ||
                  b.buffer + b.capacity <= a.buffer);
    }
  }

  for (const auto& allocation : allocations) {
    allocator->Free(allocation.buffer);
  }
}

// The Free-path guards are CHECKs, live in every build type. threadsafe style
// re-execs the binary instead of forking the (brpc/bthread/tcmalloc-linked)
// process, matching every other death test in this binary.
TEST(SlabAllocatorDeathTest, InvalidFree) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto allocator = SlabAllocator::Create(2);
  ASSERT_NE(allocator, nullptr);

  allocator->Free(nullptr);  // no-op, like free(NULL)

  {  // out-of-pool pointers, past either end of the region
    char* end = allocator->BaseAddr() + allocator->TotalSize();
    EXPECT_DEATH(allocator->Free(end), "not from this pool");
    EXPECT_DEATH(allocator->Free(allocator->BaseAddr() - 1),
                 "not from this pool");
  }

  {  // class object freed twice while its superblock stays carved
    char* pin = allocator->Alloc(4 * kKiB);
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    allocator->Free(buffer);
    EXPECT_DEATH(allocator->Free(buffer), "double free");
    allocator->Free(pin);
  }

  {  // freed pointer whose superblock already went back to the pool
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    allocator->Free(buffer);  // last object: superblock reclaimed
    EXPECT_DEATH(allocator->Free(buffer), "double free");
  }

  {  // whole superblock freed twice
    char* buffer = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(buffer, nullptr);
    allocator->Free(buffer);
    EXPECT_DEATH(allocator->Free(buffer), "double free");
  }

  {  // interior pointer of a class object
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    EXPECT_DEATH(allocator->Free(buffer + 1), "misaligned");
    allocator->Free(buffer);
  }

  {  // class-aligned but not class-sized offset into a larger class object:
     // +4 KiB into a 64 KiB object catches a check against the wrong shift
    char* buffer = allocator->Alloc(64 * kKiB);
    ASSERT_NE(buffer, nullptr);
    EXPECT_DEATH(allocator->Free(buffer + (4 * kKiB)), "misaligned");
    allocator->Free(buffer);
  }

  {  // interior pointer of a whole superblock (a separate CHECK from the
     // class path above)
    char* whole = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(whole, nullptr);
    EXPECT_DEATH(allocator->Free(whole + (4 * kKiB)), "misaligned");
    allocator->Free(whole);
  }
}

TEST(SlabAllocatorTest, IndexOf) {
  auto allocator = SlabAllocator::Create(2);
  ASSERT_NE(allocator, nullptr);
  char* base = allocator->BaseAddr();

  {  // interior pointers map to the owning superblock
    char* small = allocator->Alloc(4 * kKiB);
    char* whole = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(small, nullptr);
    ASSERT_NE(whole, nullptr);
    int index = allocator->IndexOf(small);
    EXPECT_EQ(allocator->IndexOf(small + (4 * kKiB) - 1), index);
    EXPECT_NE(allocator->IndexOf(whole), index);
    EXPECT_EQ(allocator->IndexOf(whole + kSuperblockSize - 1),
              allocator->IndexOf(whole));
    allocator->Free(small);
    allocator->Free(whole);
  }

  {  // boundaries
    EXPECT_EQ(allocator->IndexOf(base), 0);
    EXPECT_EQ(allocator->IndexOf(base + allocator->TotalSize() - 1), 1);
  }

  {  // out-of-pool pointers
    EXPECT_EQ(allocator->IndexOf(nullptr), -1);
    EXPECT_EQ(allocator->IndexOf(base + allocator->TotalSize()), -1);
    char on_stack = 0;
    EXPECT_EQ(allocator->IndexOf(&on_stack), -1);
  }
}

// Alloc on one thread, free on another: the completion-thread hand-off shape
// the RDMA / io_uring paths use. The join edges order every access here, so
// the racy unlocked tag read (I2) is exercised by the Concurrent* tests, not
// this one.
TEST(SlabAllocatorTest, CrossThreadFree) {
  auto allocator = SlabAllocator::Create(4);
  ASSERT_NE(allocator, nullptr);

  std::vector<char*> buffers;
  for (int i = 0; i < 128; ++i) {
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    buffer[0] = static_cast<char>(i);
    buffers.push_back(buffer);
  }
  char* whole = allocator->Alloc(kSuperblockSize);
  ASSERT_NE(whole, nullptr);

  std::thread freer([&] {
    for (size_t i = 0; i < buffers.size(); ++i) {
      EXPECT_EQ(buffers[i][0], static_cast<char>(i));
      allocator->Free(buffers[i]);
    }
    allocator->Free(whole);
  });
  freer.join();

  // Everything drained: all superblocks whole-allocatable again.
  std::vector<char*> drained;
  for (int i = 0; i < 4; ++i) {
    char* buffer = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(buffer, nullptr);
    drained.push_back(buffer);
  }
  for (auto* buffer : drained) {
    allocator->Free(buffer);
  }
}

TEST(SlabAllocatorTest, ConcurrentAllocFree) {
  auto allocator = SlabAllocator::Create(16);
  ASSERT_NE(allocator, nullptr);

  const std::vector<size_t> sizes = {1,         4 * kKiB,  (4 * kKiB) + 1,
                                     12 * kKiB, 64 * kKiB, 300 * kKiB,
                                     1 * kMiB,  2 * kMiB,  4 * kMiB};
  struct Held {
    char* buffer;
    size_t cap;  // rounded class capacity; last byte stamped too
    char tag;
  };
  std::vector<std::thread> threads;
  threads.reserve(8);
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);
      std::vector<Held> held;
      // 2000 iters x 8 threads keeps every superblock churning through
      // carve/reclaim for the whole run (~tens of ms), enough to hit the
      // out-of-lock Release window repeatedly.
      for (int i = 0; i < 2000; ++i) {
        size_t size = sizes[rng() % sizes.size()];
        char* buffer = allocator->Alloc(size);
        if (buffer != nullptr) {
          auto tag = static_cast<char>((t << 4) | (i % 16));
          size_t cap = ClassCapacity(size);
          buffer[0] = tag;  // both ends: catches tail overlap from aliasing
          buffer[cap - 1] = tag;
          held.push_back({.buffer = buffer, .cap = cap, .tag = tag});
        }
        if (held.size() > 4 || (buffer == nullptr && !held.empty())) {
          size_t victim = rng() % held.size();
          EXPECT_EQ(held[victim].buffer[0], held[victim].tag);
          EXPECT_EQ(held[victim].buffer[held[victim].cap - 1],
                    held[victim].tag);
          allocator->Free(held[victim].buffer);
          held.erase(held.begin() + victim);
        }
      }
      for (auto& entry : held) {
        EXPECT_EQ(entry.buffer[0], entry.tag);
        EXPECT_EQ(entry.buffer[entry.cap - 1], entry.tag);
        allocator->Free(entry.buffer);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // No leak: every superblock is whole-allocatable again.
  std::vector<char*> buffers;
  for (int i = 0; i < 16; ++i) {
    char* buffer = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(buffer, nullptr) << "superblock " << i << " leaked";
    buffers.push_back(buffer);
  }
  EXPECT_EQ(allocator->Alloc(kSuperblockSize), nullptr);
  for (auto* buffer : buffers) {
    allocator->Free(buffer);
  }
}

TEST(SlabAllocatorTest, ConcurrentReclaimChurn) {
  auto allocator = SlabAllocator::Create(4);
  ASSERT_NE(allocator, nullptr);

  // Each thread repeatedly fills then drains one class, so the 4 superblocks
  // keep churning through carve/reclaim and across classes. Including
  // kSuperblockSize races the lock-free whole path against the sub-class
  // Attach/Detach transitions for the same superblocks.
  const std::vector<size_t> class_sizes = {4 * kKiB, 64 * kKiB, 512 * kKiB,
                                           2 * kMiB, kSuperblockSize};
  std::vector<std::thread> threads;
  threads.reserve(class_sizes.size());
  for (auto size : class_sizes) {
    threads.emplace_back([&, size] {
      size_t batch = kSuperblockSize / size;
      // 300 rounds x 5 threads on 4 superblocks: ~thousands of carve/reclaim
      // cycles, enough to interleave the whole<->class tag transitions.
      for (int round = 0; round < 300; ++round) {
        std::vector<char*> held;
        for (size_t i = 0; i < batch; ++i) {
          char* buffer = allocator->Alloc(size);
          if (buffer == nullptr) {
            break;  // other classes hold the superblocks right now
          }
          held.push_back(buffer);
        }
        for (auto* buffer : held) {
          allocator->Free(buffer);
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::vector<char*> buffers;
  for (int i = 0; i < 4; ++i) {
    char* buffer = allocator->Alloc(kSuperblockSize);
    ASSERT_NE(buffer, nullptr) << "superblock " << i << " leaked";
    buffers.push_back(buffer);
  }
  for (auto* buffer : buffers) {
    allocator->Free(buffer);
  }
}

namespace {
// Captures the WARNING lines LogExhausted emits, ignoring any other logging.
class WarningCapture : public google::LogSink {
 public:
  void send(google::LogSeverity severity, const char* /*full_filename*/,
            const char* /*base_filename*/, int /*line*/,
            const google::LogMessageTime& /*logmsgtime*/, const char* message,
            size_t message_len) override {
    std::string text(message, message_len);
    if (severity == google::GLOG_WARNING &&
        text.find("Slab allocator exhausted") != std::string::npos) {
      messages.push_back(std::move(text));
    }
  }
  std::vector<std::string> messages;
};
}  // namespace

// LogExhausted logs only every 100th failure and reports the per-class
// superblock distribution; the recent RDMA slab-exhaustion incident relied on
// this diagnostic being correct.
TEST(SlabAllocatorTest, LogExhaustedSamplesAndReports) {
  auto allocator = SlabAllocator::Create(2);
  ASSERT_NE(allocator, nullptr);

  char* whole = allocator->Alloc(kSuperblockSize);  // one superblock whole
  ASSERT_NE(whole, nullptr);
  std::vector<char*> objects;  // the other fully carved into 4 KiB
  for (int i = 0; i < 1024; ++i) {
    char* buffer = allocator->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    objects.push_back(buffer);
  }

  WarningCapture sink;
  google::AddLogSink(&sink);
  for (int i = 0; i < 201; ++i) {  // failures #1, #101, #201 are logged
    EXPECT_EQ(allocator->Alloc(4 * kKiB), nullptr);
  }
  google::RemoveLogSink(&sink);

  EXPECT_EQ(sink.messages.size(), 3);
  for (const auto& message : sink.messages) {  // 4k=1 whole=1 free=0, size 4096
    EXPECT_NE(message.find("4k=1"), std::string::npos) << message;
    EXPECT_NE(message.find("whole=1"), std::string::npos) << message;
    EXPECT_NE(message.find("free=0"), std::string::npos) << message;
    EXPECT_NE(message.find("size 4096"), std::string::npos) << message;
  }

  allocator->Free(whole);
  for (auto* buffer : objects) {
    allocator->Free(buffer);
  }
}

// Latency of one alloc/free pair per class, in two regimes: `steady` pins one
// extra object so the superblock stays carved (pure bitmap fast path, the
// production shape where in-flight IOs pin superblocks); `churn` holds
// nothing, so for sub-classes every pair pays carve + reclaim + a MemoryPool
// round-trip (worst case).
TEST(SlabAllocatorTest, PerfPerClass) {
  auto allocator = SlabAllocator::Create(8);
  ASSERT_NE(allocator, nullptr);

  constexpr int kRounds = 200000;
  LOG(INFO) << "PerfPerClass (" << kRounds << " pairs per cell):";
  LOG(INFO) << "  class    steady ns/op    churn ns/op";
  for (int shift = 12; shift <= 22; ++shift) {
    size_t size = size_t{1} << shift;
    double churn = BenchPairNs(allocator.get(), size, kRounds);
    if (size < kSuperblockSize) {
      char* pin = allocator->Alloc(size);
      ASSERT_NE(pin, nullptr);
      double steady = BenchPairNs(allocator.get(), size, kRounds);
      allocator->Free(pin);
      LOG(INFO) << Row("  %-6s %12.1f %14.1f", ClassLabel(size).c_str(), steady,
                       churn);
    } else {
      LOG(INFO) << Row("  %-6s %12s %14.1f  (lock-free whole)",
                       ClassLabel(size).c_str(), "-", churn);
    }
  }
}

// Fill a whole superblock then drain it: amortized cost including carve,
// reclaim and bitmap traversal at every fill level.
TEST(SlabAllocatorTest, PerfBatchChurn) {
  auto allocator = SlabAllocator::Create(8);
  ASSERT_NE(allocator, nullptr);

  LOG(INFO) << "PerfBatchChurn (fill superblock then drain):";
  LOG(INFO) << "  class    batch    ns per pair";
  for (int shift = 12; shift <= 21; ++shift) {
    size_t size = size_t{1} << shift;
    auto batch = static_cast<int>(kSuperblockSize / size);
    int rounds = std::max(1, 200000 / batch);
    std::vector<char*> held(batch);
    double ns_per_round = 0;
    RunInBthread([&] {
      ns_per_round = TimeNsPerOp(rounds, [&] {
        for (auto& buffer : held) {
          buffer = allocator->Alloc(size);
          CHECK(buffer != nullptr);
        }
        for (auto* buffer : held) {
          allocator->Free(buffer);
        }
      });
    });

    LOG(INFO) << Row("  %-6s %8d %14.1f", ClassLabel(size).c_str(), batch,
                     ns_per_round / batch);
  }
}

// 2-128 bthread grid: all workers on one class (same-class mutex, worst case)
// swept per size, then workers spread round-robin across the 8 sub-class locks
// (per-class sharding, realistic). Reports aggregate throughput and average
// per-pair latency (threads / throughput -- one pair's time incl. lock wait).
static void RunConcurrencyGrid(SlabAllocator* allocator, int total_ops) {
  const std::vector<int> thread_counts = {2, 4, 8, 16, 32, 64, 128};
  auto latency_ns = [](int threads, double mops) {
    return static_cast<double>(threads) / mops * 1000.0;
  };
  LOG(INFO) << Row("  %-13s %8s %13s %14s", "scenario", "threads", "M pairs/s",
                   "ns/pair(avg)");

  const std::vector<size_t> sizes = {4 * kKiB,   64 * kKiB, 128 * kKiB,
                                     512 * kKiB, 1 * kMiB,   4 * kMiB};
  for (size_t size : sizes) {
    std::string label = ClassLabel(size) + " same";
    for (int threads : thread_counts) {
      double mops =
          BenchMtMops(allocator, std::vector<size_t>(threads, size), total_ops);
      LOG(INFO) << Row("  %-13s %8d %13.2f %14.1f", label.c_str(), threads, mops,
                       latency_ns(threads, mops));
    }
  }

  const std::vector<size_t> classes = {4 * kKiB,   8 * kKiB,  16 * kKiB,
                                       32 * kKiB,  64 * kKiB, 128 * kKiB,
                                       256 * kKiB, 512 * kKiB};
  for (int threads : thread_counts) {
    std::vector<size_t> mix(threads);
    for (int i = 0; i < threads; ++i) {
      mix[i] = classes[i % classes.size()];
    }
    double mops = BenchMtMops(allocator, mix, total_ops);
    LOG(INFO) << Row("  %-13s %8d %13.2f %14.1f", "distinct mix", threads, mops,
                     latency_ns(threads, mops));
  }
}

TEST(SlabAllocatorTest, PerfConcurrencyScaling) {
  bthread_setconcurrency(128);  // match cores; no-op if already higher
  auto allocator = SlabAllocator::Create(160);  // 128 workers x (pin+transient)
  ASSERT_NE(allocator, nullptr);
  LOG(INFO) << "PerfConcurrencyScaling (bthread, concurrency="
            << bthread_getconcurrency() << ", 2000000 pairs per cell):";
  RunConcurrencyGrid(allocator.get(), 2000000);
}

// Baseline: the per-IO posix_memalign/free pattern this allocator replaces
// (backed by tcmalloc in this build).
TEST(SlabAllocatorTest, PerfVsPosixMemalign) {
  auto allocator = SlabAllocator::Create(8);
  ASSERT_NE(allocator, nullptr);

  constexpr int kRounds = 200000;
  LOG(INFO) << "PerfVsPosixMemalign (" << kRounds << " pairs per cell):";
  for (size_t size : {128 * kKiB, 4 * kMiB}) {
    char* pin = size < kSuperblockSize ? allocator->Alloc(size) : nullptr;
    double slab = BenchPairNs(allocator.get(), size, kRounds);
    if (pin != nullptr) {
      allocator->Free(pin);
    }

    for (int i = 0; i < 1000; ++i) {  // warm up the malloc caches
      void* buffer = nullptr;
      CHECK_EQ(posix_memalign(&buffer, 4 * kKiB, size), 0);
      std::free(buffer);
    }
    double posix = 0;
    RunInBthread([&] {
      posix = TimeNsPerOp(kRounds, [&] {
        void* buffer = nullptr;
        CHECK_EQ(posix_memalign(&buffer, 4 * kKiB, size), 0);
        std::free(buffer);
      });
    });

    LOG(INFO) << Row("  %-6s slab %7.1f ns/op   posix_memalign %7.1f ns/op",
                     ClassLabel(size).c_str(), slab, posix);
  }
}

}  // namespace cache
}  // namespace dingofs
