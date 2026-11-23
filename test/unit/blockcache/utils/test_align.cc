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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>
#include <malloc.h>
#include <pthread.h>
#include <sched.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <latch>
#include <optional>
#include <ratio>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "blockcache/utils/align.h"
#include "blockcache/utils/cpu.h"
#include "blockcache/utils/thread.h"
#include "test/unit/blockcache/base/coverage.h"

namespace dingofs {
namespace blockcache {

constexpr uint64_t kBlockSize = 4 * kMiB;
constexpr uint64_t kIncrements = 4'000'000;

static std::optional<std::pair<int, int>> GetCorePair() {
  const std::vector<int> cpus = GetThreadCpus(::pthread_self());
  const std::vector<int> cores = RemoveSiblingCpus(cpus);
  if (cores.size() < 2) {
    return std::nullopt;
  }
  return std::pair{cores[0], cores[1]};
}

static double MeasureIncrementNs(std::atomic<uint64_t>* first, int first_cpu,
                                 std::atomic<uint64_t>* second,
                                 int second_cpu) {
  std::latch start(3);
  const auto increment = [&start](std::atomic<uint64_t>* counter, int cpu) {
    EXPECT_TRUE(BindThreadToCpu(::pthread_self(), cpu));
    start.arrive_and_wait();
    for (uint64_t i = 0; i < kIncrements; i++) {
      counter->fetch_add(1, std::memory_order_relaxed);
    }
  };
  std::thread first_thread(increment, first, first_cpu);
  std::thread second_thread(increment, second, second_cpu);
  start.arrive_and_wait();
  const auto begin = std::chrono::steady_clock::now();
  first_thread.join();
  second_thread.join();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::nano>(end - begin).count() /
         kIncrements;
}

TEST(AlignTest, Constants) {
  EXPECT_EQ(kKiB, 1024);
  EXPECT_EQ(kMiB, 1048576);
  EXPECT_EQ(kGiB, 1073741824);

  EXPECT_EQ(kCacheLineSize, 64);
  EXPECT_EQ(kIoAlign, 4096);
}

TEST(AlignTest, CacheLineSize) {
  // whole cache lines of the cpu this thread runs on, as the kernel reports
  // them, so objects aligned to it never share one
  std::ifstream file("/sys/devices/system/cpu/cpu" +
                     std::to_string(::sched_getcpu()) +
                     "/cache/index0/coherency_line_size");
  size_t line_size = 0;
  if (!(file >> line_size)) {
    GTEST_SKIP() << "the kernel reports no cache line size";
  }
  EXPECT_EQ(kCacheLineSize % line_size, size_t{0}) << line_size;
}

TEST(AlignTest, AlignUp) {
  EXPECT_EQ(AlignUp<uint64_t>(0, kIoAlign), 0);
  EXPECT_EQ(AlignUp<uint64_t>(1, kIoAlign), kIoAlign);
  EXPECT_EQ(AlignUp<uint64_t>(kIoAlign - 1, kIoAlign), kIoAlign);
  EXPECT_EQ(AlignUp<uint64_t>(kIoAlign, kIoAlign), kIoAlign);
  EXPECT_EQ(AlignUp<uint64_t>(kIoAlign + 1, kIoAlign), 2 * kIoAlign);
  EXPECT_EQ(AlignUp<uint64_t>(kBlockSize + 100, kMiB), kBlockSize + kMiB);

  {  // in the caller's own type
    EXPECT_EQ(AlignUp<uint32_t>(kBlockSize - 1, kIoAlign), kBlockSize);
    EXPECT_EQ(AlignUp<uint32_t>(kBlockSize + 1, kIoAlign),
              kBlockSize + kIoAlign);
  }
}

TEST(AlignTest, AlignDown) {
  EXPECT_EQ(AlignDown<uint64_t>(0, kIoAlign), 0);
  EXPECT_EQ(AlignDown<uint64_t>(kIoAlign - 1, kIoAlign), 0);
  EXPECT_EQ(AlignDown<uint64_t>(kIoAlign, kIoAlign), kIoAlign);
  EXPECT_EQ(AlignDown<uint64_t>(kIoAlign + 1, kIoAlign), kIoAlign);
  EXPECT_EQ(AlignDown<uint64_t>(kBlockSize + kMiB - 1, kMiB), kBlockSize);

  {  // an address inside a span maps to the span's start
    constexpr uintptr_t kSpan = 64 * kMiB;
    EXPECT_EQ(AlignDown<uintptr_t>(kSpan + 12345, 2 * kMiB), kSpan);
  }
}

TEST(AlignTest, IsAligned) {
  {  // offsets and lengths
    EXPECT_TRUE(IsAligned(uint64_t{0}, kIoAlign));
    EXPECT_TRUE(IsAligned(uint64_t{kBlockSize}, kIoAlign));
    EXPECT_FALSE(IsAligned(uint64_t{kBlockSize + 1}, kIoAlign));
    EXPECT_TRUE(IsAligned(uint32_t{128 * 1024}, kIoAlign));
    EXPECT_FALSE(IsAligned(uint32_t{kIoAlign - 1}, kIoAlign));
    EXPECT_FALSE(IsAligned(uint32_t{kIoAlign + 1}, kIoAlign));
  }

  {  // against the alignment asked for: page aligned is not MiB aligned
    EXPECT_TRUE(IsAligned(uint64_t{kBlockSize}, kMiB));
    EXPECT_FALSE(IsAligned(uint64_t{kBlockSize + kIoAlign}, kMiB));
  }

  {  // buffer addresses
    alignas(kIoAlign) static char buffer[2 * kIoAlign];
    EXPECT_TRUE(IsAligned(buffer, kIoAlign));
    EXPECT_TRUE(IsAligned(buffer + kIoAlign, kIoAlign));
    EXPECT_FALSE(IsAligned(buffer + 1, kIoAlign));
    EXPECT_FALSE(IsAligned(buffer + kIoAlign - 1, kIoAlign));
    EXPECT_TRUE(IsAligned(static_cast<const void*>(nullptr), kIoAlign));
  }
}

TEST(AlignTest, AlignedAlloc) {
  {  // a block buffer, page aligned and writable end to end
    char* buffer = AlignedAlloc(kBlockSize);
    ASSERT_NE(buffer, nullptr);
    EXPECT_TRUE(IsAligned(buffer, kIoAlign));
    std::memset(buffer, 0x5a, kBlockSize);
    EXPECT_EQ(buffer[kBlockSize - 1], 0x5a);
    std::free(buffer);
  }

  {  // the size is rounded up to whole pages, so a direct write of the last
     // page stays inside the buffer
    char* buffer = AlignedAlloc(kIoAlign + 1);
    ASSERT_NE(buffer, nullptr);
    EXPECT_TRUE(IsAligned(buffer, kIoAlign));
    EXPECT_GE(malloc_usable_size(buffer), 2 * kIoAlign);
    std::free(buffer);
  }

  {  // a caller-chosen alignment, which rounds the size up as well
    char* buffer = AlignedAlloc(1, 2 * kMiB);
    ASSERT_NE(buffer, nullptr);
    EXPECT_TRUE(IsAligned(buffer, 2 * kMiB));
    EXPECT_GE(malloc_usable_size(buffer), 2 * kMiB);
    std::free(buffer);
  }

  {  // a size no memory can back is a nullptr, not a crash
    EXPECT_EQ(AlignedAlloc(SIZE_MAX / 2), nullptr);
  }
}

TEST(AlignPerfTest, FalseSharing) {
  if (CoverageRequested()) {
    GTEST_SKIP() << "gcov's counters in the loop are shared by both threads";
  }

  const auto pair = GetCorePair();
  if (!pair.has_value()) {
    GTEST_SKIP() << "no two cores to run on";
  }
  const auto [first_cpu, second_cpu] = *pair;

  // Some cpus prefetch cache lines in pairs, which is why align.h asks
  // whether 128 would be better.
  constexpr size_t kMaxApart = 2 * kCacheLineSize;
  struct Case {
    size_t apart;  // bytes from the first counter to the second
    const char* layout;
  };
  const Case cases[] = {
      {.apart = sizeof(uint64_t), .layout = "one cache line"},
      {.apart = kCacheLineSize, .layout = "kCacheLineSize apart"},
      {.apart = kMaxApart, .layout = "2 * kCacheLineSize apart"},
  };
  std::printf("[two threads on cpus %d and %d, two cores, %" PRIu64
              " increments each]\n",
              first_cpu, second_cpu, kIncrements);
  for (const Case& c : cases) {
    // counters[0] starts a 2 * kCacheLineSize block, so the one kCacheLineSize
    // away is the other line of that block and the one kMaxApart away is in
    // the next block
    constexpr size_t kCounters = (kMaxApart / sizeof(uint64_t)) + 1;
    alignas(kMaxApart) std::array<std::atomic<uint64_t>, kCounters> counters{};
    std::atomic<uint64_t>* first = counters.data();
    std::atomic<uint64_t>* second = &counters[c.apart / sizeof(uint64_t)];
    const double ns = MeasureIncrementNs(first, first_cpu, second, second_cpu);
    std::printf("  counters %3zu bytes apart, %-24s: %6.1f ns per increment\n",
                c.apart, c.layout, ns);
  }
}

}  // namespace blockcache
}  // namespace dingofs
