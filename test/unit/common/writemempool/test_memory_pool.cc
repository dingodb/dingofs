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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/writemempool/memory_pool.h"

namespace dingofs {

namespace {

// Drains the pool and returns the full set of buffer addresses. Useful for
// building an index → address map for ownership tracking in concurrent tests.
std::vector<char*> DrainPool(MemoryPool* pool, size_t expected_count) {
  std::vector<char*> bufs;
  bufs.reserve(expected_count);
  char* b;
  while ((b = pool->Require()) != nullptr) {
    bufs.push_back(b);
  }
  return bufs;
}

}  // namespace

// ----- functional -----

TEST(MemoryPoolTest, CreateSucceeds) {
  auto pool = MemoryPool::Create(4096, 16);
  ASSERT_NE(pool, nullptr);
}

TEST(MemoryPoolTest, RequireReturnsDistinctAddresses) {
  constexpr size_t kBufferSize = 4096;
  constexpr size_t kBufferCount = 64;
  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  auto bufs = DrainPool(pool.get(), kBufferCount);
  ASSERT_EQ(bufs.size(), kBufferCount);

  std::unordered_set<char*> uniq(bufs.begin(), bufs.end());
  EXPECT_EQ(uniq.size(), kBufferCount) << "duplicate buffer returned";
}

TEST(MemoryPoolTest, RequireExhaustionReturnsNullptr) {
  constexpr size_t kBufferCount = 8;
  auto pool = MemoryPool::Create(64, kBufferCount);
  ASSERT_NE(pool, nullptr);

  auto bufs = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(bufs.size(), kBufferCount);
  EXPECT_EQ(pool->Require(), nullptr);
}

TEST(MemoryPoolTest, ReleasedBufferIsReacquirable) {
  auto pool = MemoryPool::Create(64, 4);
  ASSERT_NE(pool, nullptr);

  auto bufs = DrainPool(pool.get(), 4);
  ASSERT_EQ(bufs.size(), 4);
  ASSERT_EQ(pool->Require(), nullptr);

  pool->Release(bufs[1]);
  char* re = pool->Require();
  EXPECT_NE(re, nullptr);
  EXPECT_EQ(re, bufs[1]);
  EXPECT_EQ(pool->Require(), nullptr);
}

TEST(MemoryPoolTest, AddressesAreContiguousAndAligned) {
  constexpr size_t kBufferSize = 4096;
  constexpr size_t kBufferCount = 32;
  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  auto bufs = DrainPool(pool.get(), kBufferCount);
  ASSERT_EQ(bufs.size(), kBufferCount);
  std::sort(bufs.begin(), bufs.end());

  for (size_t i = 1; i < bufs.size(); ++i) {
    EXPECT_EQ(bufs[i] - bufs[i - 1], static_cast<ptrdiff_t>(kBufferSize));
  }
}

TEST(MemoryPoolTest, BufferDataIsWritable) {
  constexpr size_t kBufferSize = 256;
  auto pool = MemoryPool::Create(kBufferSize, 4);
  ASSERT_NE(pool, nullptr);

  char* b = pool->Require();
  ASSERT_NE(b, nullptr);
  // Skip the first sizeof(uint32_t) bytes — those are the intrusive free-list
  // "next" pointer and contain stale data on a freshly-acquired buffer.
  std::memset(b + sizeof(uint32_t), 0xAB, kBufferSize - sizeof(uint32_t));
  for (size_t i = sizeof(uint32_t); i < kBufferSize; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(b[i]), 0xAB);
  }
  pool->Release(b);
}

TEST(MemoryPoolTest, FullCycleDoesNotLeak) {
  constexpr size_t kBufferCount = 128;
  auto pool = MemoryPool::Create(64, kBufferCount);
  ASSERT_NE(pool, nullptr);

  for (int round = 0; round < 4; ++round) {
    auto bufs = DrainPool(pool.get(), kBufferCount);
    ASSERT_EQ(bufs.size(), kBufferCount);
    EXPECT_EQ(pool->Require(), nullptr);
    for (char* b : bufs) pool->Release(b);
  }
}

// ----- concurrent correctness -----

// Stress test: many threads do Require → hold → Release in a loop. A separate
// ownership map tracks which thread (1..N) currently holds each buffer; a CAS
// from 0→tid on acquire and tid→0 on release proves no buffer is ever handed
// out twice. Any deviation flags double-allocation or out-of-range addresses.
TEST(MemoryPoolTest, ConcurrentNoDoubleAllocation) {
  constexpr size_t kBufferSize = 128;
  constexpr size_t kBufferCount = 512;
  constexpr int kNumThreads = 16;
  constexpr int kIters = 20000;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  // Build sorted address table to map buffer addr → slot index.
  auto all = DrainPool(pool.get(), kBufferCount);
  ASSERT_EQ(all.size(), kBufferCount);
  std::sort(all.begin(), all.end());
  for (char* b : all) pool->Release(b);

  auto IndexOf = [&all](char* buf) -> int {
    auto it = std::lower_bound(all.begin(), all.end(), buf);
    if (it == all.end() || *it != buf) return -1;
    return static_cast<int>(it - all.begin());
  };

  std::vector<std::atomic<int>> owners(kBufferCount);
  for (auto& o : owners) o.store(0, std::memory_order_relaxed);

  std::atomic<int> double_alloc{0};
  std::atomic<int> bad_release{0};
  std::atomic<int> bad_addr{0};
  std::atomic<int> require_null{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 1; t <= kNumThreads; ++t) {
    threads.emplace_back([&, tid = t]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < kIters; ++i) {
        char* buf = pool->Require();
        if (buf == nullptr) {
          require_null.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        int idx = IndexOf(buf);
        if (idx < 0) {
          bad_addr.fetch_add(1, std::memory_order_relaxed);
          pool->Release(buf);
          continue;
        }
        int expected = 0;
        if (!owners[idx].compare_exchange_strong(expected, tid,
                                                 std::memory_order_acq_rel)) {
          double_alloc.fetch_add(1, std::memory_order_relaxed);
        }
        expected = tid;
        if (!owners[idx].compare_exchange_strong(expected, 0,
                                                 std::memory_order_acq_rel)) {
          bad_release.fetch_add(1, std::memory_order_relaxed);
        }
        pool->Release(buf);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();

  EXPECT_EQ(double_alloc.load(), 0);
  EXPECT_EQ(bad_release.load(), 0);
  EXPECT_EQ(bad_addr.load(), 0);

  // All slots must be free again.
  for (size_t i = 0; i < kBufferCount; ++i) {
    EXPECT_EQ(owners[i].load(), 0) << "slot " << i << " not released";
  }

  // Verify pool still has full inventory available.
  auto remaining = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(remaining.size(), kBufferCount);

  LOG(INFO) << "ConcurrentNoDoubleAllocation: require_null="
            << require_null.load() << " (transient exhaustion is fine)";
}

// Producer/consumer: half the threads only Require, hand buffers via a
// lock-free queue (mutex-protected for simplicity), and the other half only
// Release. Exercises the case where the acquiring shard differs from the
// releasing shard, exposing potential shard-isolation bugs.
TEST(MemoryPoolTest, ConcurrentProducerConsumer) {
  constexpr size_t kBufferSize = 64;
  constexpr size_t kBufferCount = 256;
  constexpr int kProducers = 8;
  constexpr int kConsumers = 8;
  constexpr int kItemsPerProducer = 10000;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  std::vector<char*> queue;
  std::mutex q_mu;
  std::atomic<int> produced{0};
  std::atomic<int> consumed{0};
  const int total = kProducers * kItemsPerProducer;

  std::vector<std::thread> threads;
  threads.reserve(kProducers + kConsumers);

  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&]() {
      int made = 0;
      while (made < kItemsPerProducer) {
        char* buf = pool->Require();
        if (buf == nullptr) {
          std::this_thread::yield();
          continue;
        }
        {
          std::lock_guard<std::mutex> lk(q_mu);
          queue.push_back(buf);
        }
        produced.fetch_add(1, std::memory_order_relaxed);
        ++made;
      }
    });
  }

  for (int c = 0; c < kConsumers; ++c) {
    threads.emplace_back([&]() {
      while (consumed.load(std::memory_order_relaxed) < total) {
        char* buf = nullptr;
        {
          std::lock_guard<std::mutex> lk(q_mu);
          if (!queue.empty()) {
            buf = queue.back();
            queue.pop_back();
          }
        }
        if (buf == nullptr) {
          std::this_thread::yield();
          continue;
        }
        pool->Release(buf);
        consumed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& th : threads) th.join();

  EXPECT_EQ(produced.load(), total);
  EXPECT_EQ(consumed.load(), total);

  auto remaining = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(remaining.size(), kBufferCount);
}

// Exhaustion + refill stress: pool sized so threads frequently hit empty
// shards and exercise the work-stealing fall-through in Require().
TEST(MemoryPoolTest, ConcurrentExhaustionAndRefill) {
  constexpr size_t kBufferSize = 64;
  constexpr size_t kBufferCount = 64;  // small pool, high contention
  constexpr int kNumThreads = 32;
  constexpr int kIters = 5000;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  std::atomic<int> total_acquired{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      // Each thread holds up to kHold buffers at a time before releasing,
      // forcing temporary exhaustion of individual shards.
      constexpr int kHold = 4;
      std::vector<char*> held;
      held.reserve(kHold);
      for (int i = 0; i < kIters; ++i) {
        char* buf = pool->Require();
        if (buf != nullptr) {
          held.push_back(buf);
          total_acquired.fetch_add(1, std::memory_order_relaxed);
        }
        if (held.size() >= kHold || (buf == nullptr && !held.empty())) {
          for (char* h : held) pool->Release(h);
          held.clear();
        }
      }
      for (char* h : held) pool->Release(h);
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();

  // No correctness assertions on the count — under contention some Require
  // calls observe an "empty" snapshot and return null. What matters is the
  // pool is intact afterwards.
  auto remaining = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(remaining.size(), kBufferCount);

  LOG(INFO) << "ExhaustionAndRefill: total_acquired=" << total_acquired.load()
            << " (out of " << (kNumThreads * kIters) << " attempts)";
}

// Exercises the FlushCacheToShards path: single thread acquires all buffers
// then releases them all. Since the per-thread cache caps at kCacheCap=8,
// any pool larger than 8 will overflow the cache and trigger flushes during
// the release loop.
TEST(MemoryPoolTest, CacheOverflowTriggersFlush) {
  constexpr size_t kBufferSize = 64;
  // 100 >> kCacheCap=8, so releases will overflow the cache > 10 times.
  constexpr size_t kBufferCount = 100;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  auto bufs = DrainPool(pool.get(), kBufferCount);
  ASSERT_EQ(bufs.size(), kBufferCount);
  EXPECT_EQ(pool->Require(), nullptr);

  // Release all — cache fills to kCacheCap, flushes half to shards, refills,
  // ...
  for (char* b : bufs) pool->Release(b);

  // Drain again — must recover all 100 buffers (sum of cache + shards).
  auto bufs2 = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(bufs2.size(), kBufferCount);

  // And every buffer must come from the original address set.
  std::unordered_set<char*> orig(bufs.begin(), bufs.end());
  for (char* b : bufs2) {
    EXPECT_EQ(orig.count(b), 1u) << "got address not in original pool";
  }
}

// Each thread writes a (tid, iter) tag into its buffer and verifies the tag
// is intact after a brief work loop. Catches double-allocation that the
// owners[] CAS check might race past — if two threads ever hold the same
// buffer, their tags collide and one read-back will fail.
TEST(MemoryPoolTest, DataIntegrityUnderContention) {
  constexpr size_t kBufferSize = 256;
  constexpr size_t kBufferCount = 256;
  constexpr int kNumThreads = 32;
  constexpr int kIters = 5000;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  std::atomic<int> corruptions{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 1; t <= kNumThreads; ++t) {
    threads.emplace_back([&, tid = t]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < kIters; ++i) {
        char* buf = pool->Require();
        if (buf == nullptr) continue;
        // Write our (tid, iter) tag past the intrusive free-list area.
        uint64_t tag =
            (static_cast<uint64_t>(tid) << 32) | static_cast<uint64_t>(i);
        std::memcpy(buf + sizeof(uint32_t), &tag, sizeof(tag));
        // Brief hold to widen the race window.
        for (volatile int x = 0; x < 16; ++x) {
        }
        uint64_t check;
        std::memcpy(&check, buf + sizeof(uint32_t), sizeof(check));
        if (check != tag) {
          corruptions.fetch_add(1, std::memory_order_relaxed);
        }
        pool->Release(buf);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();

  EXPECT_EQ(corruptions.load(), 0);
}

// Thread churn: spawns batches of new threads sequentially, each doing
// Require/Release work and then exiting. The thread_local slot ID is
// allocated from a monotonic counter, so total threads >> kNumCaches=128
// forces slot reuse — new threads inherit slots from exited ones (whose
// caches may have stranded buffers). End-of-test drain must still recover
// every buffer via cache stealing.
TEST(MemoryPoolTest, ThreadChurnNoBufferLoss) {
  constexpr size_t kBufferSize = 64;
  constexpr size_t kBufferCount = 256;
  constexpr int kBatches = 25;
  constexpr int kThreadsPerBatch = 16;  // 25*16 = 400 unique threads >> 128
  constexpr int kItersPerThread = 500;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  for (int batch = 0; batch < kBatches; ++batch) {
    std::vector<std::thread> threads;
    threads.reserve(kThreadsPerBatch);
    for (int t = 0; t < kThreadsPerBatch; ++t) {
      threads.emplace_back([&]() {
        std::vector<char*> held;
        held.reserve(4);
        for (int i = 0; i < kItersPerThread; ++i) {
          char* buf = pool->Require();
          if (buf != nullptr) held.push_back(buf);
          if (held.size() >= 4 || (buf == nullptr && !held.empty())) {
            for (char* h : held) pool->Release(h);
            held.clear();
          }
        }
        for (char* h : held) pool->Release(h);
      });
    }
    for (auto& th : threads) th.join();
  }

  // All worker threads have exited; their TLS caches may still hold buffers.
  // The drain below must rescue every one via cache stealing.
  auto remaining = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(remaining.size(), kBufferCount);
}

// Slot-collision stress: spawns 256 concurrent threads on 128 cache slots,
// guaranteeing every slot has ≥2 contending threads. Exercises the
// "lock contested → fall back to shard" path in both Require and Release.
TEST(MemoryPoolTest, MoreThreadsThanCaches) {
  constexpr size_t kBufferSize = 128;
  constexpr size_t kBufferCount = 1024;
  constexpr int kNumThreads = 256;  // > kNumCaches=128
  constexpr int kIters = 2000;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  auto all = DrainPool(pool.get(), kBufferCount);
  ASSERT_EQ(all.size(), kBufferCount);
  std::sort(all.begin(), all.end());
  for (char* b : all) pool->Release(b);

  auto IndexOf = [&all](char* buf) -> int {
    auto it = std::lower_bound(all.begin(), all.end(), buf);
    if (it == all.end() || *it != buf) return -1;
    return static_cast<int>(it - all.begin());
  };

  std::vector<std::atomic<int>> owners(kBufferCount);
  for (auto& o : owners) o.store(0, std::memory_order_relaxed);

  std::atomic<int> double_alloc{0};
  std::atomic<int> bad_release{0};
  std::atomic<int> bad_addr{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 1; t <= kNumThreads; ++t) {
    threads.emplace_back([&, tid = t]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < kIters; ++i) {
        char* buf = pool->Require();
        if (buf == nullptr) continue;
        int idx = IndexOf(buf);
        if (idx < 0) {
          bad_addr.fetch_add(1, std::memory_order_relaxed);
          pool->Release(buf);
          continue;
        }
        int expected = 0;
        if (!owners[idx].compare_exchange_strong(expected, tid,
                                                 std::memory_order_acq_rel)) {
          double_alloc.fetch_add(1, std::memory_order_relaxed);
        }
        expected = tid;
        if (!owners[idx].compare_exchange_strong(expected, 0,
                                                 std::memory_order_acq_rel)) {
          bad_release.fetch_add(1, std::memory_order_relaxed);
        }
        pool->Release(buf);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();

  EXPECT_EQ(double_alloc.load(), 0);
  EXPECT_EQ(bad_release.load(), 0);
  EXPECT_EQ(bad_addr.load(), 0);

  auto remaining = DrainPool(pool.get(), kBufferCount);
  EXPECT_EQ(remaining.size(), kBufferCount);
}

// Extreme contention: pool with exactly 1 buffer. The single buffer hops
// between caches/shards as threads compete. Verifies (a) only one thread
// ever holds it at a time, and (b) the buffer is recoverable at the end.
TEST(MemoryPoolTest, PoolSizeOneHighContention) {
  constexpr int kNumThreads = 16;
  constexpr int kIters = 2000;

  auto pool = MemoryPool::Create(64, 1);
  ASSERT_NE(pool, nullptr);

  std::atomic<int> overlap{0};
  std::atomic<int> got_buffer{0};
  std::atomic<int> in_use{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < kIters; ++i) {
        char* buf = pool->Require();
        if (buf == nullptr) continue;
        int prev = in_use.exchange(1, std::memory_order_acq_rel);
        if (prev != 0) overlap.fetch_add(1, std::memory_order_relaxed);
        got_buffer.fetch_add(1, std::memory_order_relaxed);
        for (volatile int x = 0; x < 8; ++x) {
        }
        in_use.store(0, std::memory_order_release);
        pool->Release(buf);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();

  EXPECT_EQ(overlap.load(), 0) << "two threads held the only buffer at once";
  EXPECT_GT(got_buffer.load(), 0) << "no thread ever acquired the buffer";

  // Pool must still have exactly 1 buffer.
  char* b = pool->Require();
  EXPECT_NE(b, nullptr);
  EXPECT_EQ(pool->Require(), nullptr);
  if (b != nullptr) pool->Release(b);
}

// ----- performance -----

namespace {

double RunBenchmark(MemoryPool* pool, int num_threads, int iters_per_thread) {
  std::atomic<bool> start{false};
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, iters_per_thread]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < iters_per_thread; ++i) {
        char* buf = pool->Require();
        if (buf != nullptr) pool->Release(buf);
      }
    });
  }
  auto t0 = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();
  auto t1 = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(t1 - t0).count();
  double ops = static_cast<double>(num_threads) * iters_per_thread;
  return ops / sec;
}

}  // namespace

// Reports require+release throughput at several concurrency levels. Not
// asserting numbers — just prints for visibility. Tunable iter counts keep
// total runtime under ~1s on a modest machine.
TEST(MemoryPoolPerf, RequireReleaseThroughput) {
  constexpr size_t kBufferSize = 4096;
  constexpr size_t kBufferCount = 4096;  // 16 MiB pool, plenty for all threads
  constexpr int kIters = 200000;

  auto pool = MemoryPool::Create(kBufferSize, kBufferCount);
  ASSERT_NE(pool, nullptr);

  const int thread_counts[] = {1, 2, 4, 8, 16, 32, 64};
  for (int n : thread_counts) {
    double ops_per_sec = RunBenchmark(pool.get(), n, kIters);
    LOG(INFO) << "Perf threads=" << n
              << " ops/sec=" << static_cast<long long>(ops_per_sec)
              << " ns/op=" << (1e9 / ops_per_sec * n);
  }
}

}  // namespace dingofs
