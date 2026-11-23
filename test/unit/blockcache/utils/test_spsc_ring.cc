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
#include <pthread.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <latch>
#include <memory>
#include <optional>
#include <ratio>
#include <thread>
#include <vector>

#include "blockcache/core/runtime/mesh.h"
#include "blockcache/utils/cpu.h"
#include "blockcache/utils/spsc_ring.h"
#include "blockcache/utils/thread.h"
#include "test/unit/blockcache/base/coverage.h"

namespace dingofs {
namespace blockcache {

// The ring is one direction of a mesh channel between two shards; these
// stand in for the work it carries.
class NoopWork final : public MeshWork {
 public:
  void Run() noexcept override {}
  void OnComplete() noexcept override {}
};

using Ring = SpscRing<MeshWork*, MeshChannel::kDepth>;
constexpr uint32_t kDepth = MeshChannel::kDepth;

// Items are pushed in rotation over more than a ring holds, so a doubled or
// reordered one shows; a power of two, so picking one is a mask.
constexpr uint32_t kWorks = 1024;

TEST(SpscRingTest, PushBack) {
  std::vector<NoopWork> works(kDepth + 1);
  Ring ring;
  uint32_t consumed = 0;
  const auto consume = [&consumed](MeshWork*) { consumed++; };

  {  // up to the depth in flight, then refused until the consumer drains
    for (uint32_t i = 0; i < kDepth; i++) {
      ASSERT_TRUE(ring.PushBack(&works[i])) << i;
    }
    EXPECT_FALSE(ring.PushBack(&works[kDepth]));
    EXPECT_FALSE(ring.PushBack(&works[kDepth]));
  }

  {  // what was drained can be pushed into again
    EXPECT_EQ(ring.ConsumeAll(consume), kDepth);
    EXPECT_TRUE(ring.PushBack(&works[kDepth]));
    EXPECT_EQ(ring.ConsumeAll(consume), 1);
    EXPECT_EQ(consumed, kDepth + 1);
  }
}

TEST(SpscRingTest, ConsumeAll) {
  std::vector<NoopWork> works(3);
  Ring ring;
  std::vector<MeshWork*> got;
  const auto consume = [&got](MeshWork* work) { got.push_back(work); };

  {  // nothing pushed, nothing to do
    EXPECT_EQ(ring.ConsumeAll(consume), 0);
    EXPECT_TRUE(got.empty());
  }

  {  // everything pushed so far, in order, and each once
    for (uint32_t i = 0; i < 3; i++) {
      ASSERT_TRUE(ring.PushBack(&works[i]));
    }
    EXPECT_EQ(ring.ConsumeAll(consume), 3);
    EXPECT_EQ(ring.ConsumeAll(consume), 0);
    ASSERT_EQ(got.size(), 3);
    for (uint32_t i = 0; i < 3; i++) {
      EXPECT_EQ(got[i], &works[i]) << i;
    }
  }
}

TEST(SpscRingTest, WrapAround) {
  std::vector<NoopWork> works(kDepth);
  NoopWork filler;
  Ring ring;
  std::vector<MeshWork*> got;
  const auto consume = [&got](MeshWork* work) { got.push_back(work); };

  // Move both ends to the second-to-last slot.
  for (uint32_t i = 0; i < kDepth - 2; i++) {
    ASSERT_TRUE(ring.PushBack(&filler));
  }
  ASSERT_EQ(ring.ConsumeAll(consume), kDepth - 2);
  got.clear();

  // A full ring now takes the last two slots, then wraps to slots 0 to
  // kDepth - 3, which still hold the filler.
  for (uint32_t i = 0; i < kDepth; i++) {
    ASSERT_TRUE(ring.PushBack(&works[i])) << i;
  }
  EXPECT_EQ(ring.ConsumeAll(consume), kDepth);
  ASSERT_EQ(got.size(), kDepth);
  for (uint32_t i = 0; i < kDepth; i++) {
    EXPECT_EQ(got[i], &works[i]) << i;
  }
}

TEST(SpscRingTest, ProducerAndConsumer) {
  // One shard submits to another as fast as it can: everything arrives,
  // once and in order, however often the ring runs full.
  constexpr uint32_t kTransfers = 1000000;
  std::vector<NoopWork> works(kWorks);
  Ring ring;

  std::thread producer([&] {
    for (uint32_t i = 0; i < kTransfers; i++) {
      while (!ring.PushBack(&works[i % kWorks])) {
        std::this_thread::yield();
      }
    }
  });

  uint32_t received = 0;
  const auto consume = [&](MeshWork* work) {
    EXPECT_EQ(work, &works[received % kWorks]) << received;
    received++;
  };
  while (received < kTransfers) {
    if (ring.ConsumeAll(consume) == 0) {
      std::this_thread::yield();
    }
  }
  producer.join();

  EXPECT_EQ(received, kTransfers);
  EXPECT_EQ(ring.ConsumeAll(consume), 0);
}

// Each row is the median of three runs: outside load now and then stalls one.
constexpr int kRuns = 3;

// MeshLink::kFlushBatch (private): a link flushes early once it holds this
// many calls.
constexpr uint32_t kFlushBatch = 16;

// Two cpus for the two sides of a ring, and how far apart they are.
struct CpuPair {
  const char* placement;
  int first;
  int second;
};

// The first of `cpus` whose smt sibling is one of `cpus` too.
static std::optional<CpuPair> GetSiblingPair(const std::vector<int>& cpus) {
  for (const int cpu : cpus) {
    const int sibling = GetSiblingCpu(cpu);
    if (sibling != kNoCpu && std::ranges::find(cpus, sibling) != cpus.end()) {
      return CpuPair{
          .placement = "smt siblings", .first = cpu, .second = sibling};
    }
  }
  return std::nullopt;
}

// The first two of `cores` on one numa node, or on two different ones.
static std::optional<CpuPair> GetCorePair(const std::vector<int>& cores,
                                          bool same_node) {
  std::vector<int> nodes;
  nodes.reserve(cores.size());
  for (const int core : cores) {
    nodes.push_back(GetNumaNode(core));
  }
  for (size_t i = 0; i < cores.size(); i++) {
    for (size_t j = i + 1; j < cores.size(); j++) {
      if ((nodes[i] == nodes[j]) == same_node) {
        return CpuPair{
            .placement = same_node ? "one numa node" : "two numa nodes",
            .first = cores[i],
            .second = cores[j]};
      }
    }
  }
  return std::nullopt;
}

// The placements this thread's cpus offer, nearest first. None puts both
// sides on one cpu: two threads spinning there progress only when it
// switches between them.
static std::vector<CpuPair> GetCpuPairs() {
  const std::vector<int> cpus = GetThreadCpus(::pthread_self());
  const std::vector<int> cores = RemoveSiblingCpus(cpus);
  const std::optional<CpuPair> placements[] = {
      GetSiblingPair(cpus),
      GetCorePair(cores, /*same_node=*/true),
      GetCorePair(cores, /*same_node=*/false),
  };

  std::vector<CpuPair> pairs;
  for (const std::optional<CpuPair>& placement : placements) {
    if (placement.has_value()) {
      pairs.push_back(*placement);
    }
  }
  return pairs;
}

// Runs `first` on cpu pair.first and `second` on cpu pair.second, each in a
// thread of its own, once both threads are bound. False, and neither runs,
// when either cannot be bound (its cpu has gone offline since the pair was
// picked, say): unbound, the two could share a cpu after all.
template <typename First, typename Second>
static bool RunOnCpuPair(const CpuPair& pair, const First& first,
                         const Second& second) {
  std::atomic<bool> unbound{false};
  std::latch tried(2);  // until both threads tried to bind
  const auto run_on = [&unbound, &tried](int cpu, const auto& side) {
    if (!BindThreadToCpu(::pthread_self(), cpu)) {
      unbound.store(true, std::memory_order_relaxed);
    }
    tried.arrive_and_wait();
    if (!unbound.load(std::memory_order_relaxed)) {
      side();
    }
  };
  std::thread first_thread([&] { run_on(pair.first, first); });
  std::thread second_thread([&] { run_on(pair.second, second); });
  first_thread.join();
  second_thread.join();
  return !unbound.load(std::memory_order_relaxed);
}

struct StreamRun {
  double ns_per_push = 0;
  uint32_t misplaced = 0;  // items other than the one pushed in that place
  uint32_t drains = 0;     // ConsumeAll calls that returned items
  uint32_t refused = 0;    // pushes that found the ring full
};

// A push costs 15 to 47 ns from one ring to the next on the same two cores,
// likely by the L3 slices the ring's lines hash to, so a run streams through
// many rings.
constexpr uint32_t kStreamRings = 64;
constexpr uint32_t kPushesPerRing = 2048;
constexpr uint32_t kStreamPushes = kStreamRings * kPushesPerRing;

// The producer (first cpu) pushes as fast as the consumer (second cpu)
// drains, ring after ring. Both spin on the rings, a CpuRelax per refused
// push or empty ConsumeAll, as a shard does per idle poll. Their loops use
// locals only: what a loop reaches through the lambda's captures is reloaded
// from memory at every push. Empty when the two cannot be bound.
static std::optional<StreamRun> MeasureStream(const CpuPair& pair) {
  const auto rings = std::make_unique<Ring[]>(kStreamRings);
  std::vector<NoopWork> works(kWorks);
  std::atomic<bool> polling{false};
  StreamRun run;
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  const auto consumer = [&] {
    Ring* const stream = rings.get();
    NoopWork* const items = works.data();
    uint32_t received = 0;
    uint32_t misplaced = 0;
    uint32_t drains = 0;
    const auto receive = [&](MeshWork* work) {
      misplaced += work != &items[received % kWorks] ? 1 : 0;
      received++;
    };
    polling.store(true, std::memory_order_release);
    for (uint32_t r = 0; r < kStreamRings; r++) {
      while (received < (r + 1) * kPushesPerRing) {
        if (stream[r].ConsumeAll(receive) != 0) {
          drains++;
        } else {
          CpuRelax();
        }
      }
    }
    end = std::chrono::steady_clock::now();
    run.misplaced = misplaced;
    run.drains = drains;
  };

  const auto producer = [&] {
    Ring* const stream = rings.get();
    NoopWork* const items = works.data();
    uint32_t refused = 0;
    while (!polling.load(std::memory_order_acquire)) {
      CpuRelax();
    }
    begin = std::chrono::steady_clock::now();
    for (uint32_t i = 0; i < kStreamPushes; i++) {
      Ring& ring = stream[i / kPushesPerRing];
      MeshWork* work = &items[i % kWorks];
      if (!ring.PushBack(work)) {
        refused++;
        while (!ring.PushBack(work)) {
          CpuRelax();
        }
      }
    }
    run.refused = refused;
  };

  if (!RunOnCpuPair(pair, producer, consumer)) {
    return std::nullopt;
  }
  const std::chrono::duration<double, std::nano> elapsed = end - begin;
  run.ns_per_push = elapsed.count() / kStreamPushes;
  return run;
}

TEST(SpscRingPerfTest, Throughput) {
  if (CoverageRequested()) {
    GTEST_SKIP() << "gcov's counters in PushBack and ConsumeAll are shared by "
                    "both threads";
  }
  const std::vector<CpuPair> pairs = GetCpuPairs();
  if (pairs.empty()) {
    GTEST_SKIP() << "no two cpus to run on";
  }

  std::printf(
      "[spsc ring throughput, depth %u, %u rings in turn, median of %d "
      "runs]\n",
      kDepth, kStreamRings, kRuns);
  for (const CpuPair& pair : pairs) {
    std::array<StreamRun, kRuns> runs;
    for (StreamRun& run : runs) {
      const std::optional<StreamRun> measured = MeasureStream(pair);
      if (!measured.has_value()) {
        GTEST_SKIP() << "cannot bind to cpus " << pair.first << " and "
                     << pair.second;
      }
      run = *measured;
      EXPECT_EQ(run.misplaced, 0) << pair.placement;
    }
    std::ranges::sort(runs, {}, &StreamRun::ns_per_push);
    const StreamRun& median = runs[kRuns / 2];
    std::printf(
        "  %-14s cpus %3d -> %3d : %5.1f ns per push, %4.1f items per drain, "
        "%4.2f%% of pushes refused\n",
        pair.placement, pair.first, pair.second, median.ns_per_push,
        static_cast<double>(kStreamPushes) / median.drains,
        100.0 * median.refused / kStreamPushes);
  }
}

struct RoundTripRun {
  double ns_per_round_trip = 0;
  uint32_t misplaced = 0;  // calls back out of the order they went
};

// A channel's round trip moves with its address, a full ring's by up to a
// third, so rounds go over the channels in turn; a power of two, so picking
// one is a mask.
constexpr uint32_t kChannels = 16;

// The caller (first cpu) pushes `batch` calls onto a channel's pending ring
// and spins on its completed ring until all are back; the callee (second
// cpu) answers each call as it drains pending. No notify word, no reactor:
// both spin on the rings, a CpuRelax per empty poll. `batch` fits in a ring,
// since the caller pushes all of it before it drains completed. `rounds` are
// timed after a tenth as many untimed. Empty when the two cannot be bound.
static std::optional<RoundTripRun> MeasureRoundTrip(const CpuPair& pair,
                                                    uint32_t batch,
                                                    uint32_t rounds) {
  const auto channels = std::make_unique<MeshChannel[]>(kChannels);
  std::vector<NoopWork> works(batch);
  const uint32_t warmup = rounds / 10;
  std::atomic<bool> polling{false};
  RoundTripRun run;

  const auto callee = [&] {
    polling.store(true, std::memory_order_release);
    for (uint32_t round = 0; round < warmup + rounds; round++) {
      MeshChannel& channel = channels[round % kChannels];
      const auto answer = [&channel](MeshWork* work) {
        while (!channel.completed.PushBack(work)) {
          CpuRelax();
        }
      };
      for (uint32_t answered = 0; answered < batch;) {
        const uint32_t n = channel.pending.ConsumeAll(answer);
        if (n == 0) {
          CpuRelax();
        }
        answered += n;
      }
    }
  };

  const auto caller = [&] {
    while (!polling.load(std::memory_order_acquire)) {
      CpuRelax();
    }
    // Counted here, not in `run`: that shares a cache line with what the
    // callee reads.
    uint32_t misplaced = 0;
    const auto round_trip = [&](uint32_t round) {
      MeshChannel& channel = channels[round % kChannels];
      for (NoopWork& work : works) {
        while (!channel.pending.PushBack(&work)) {
          CpuRelax();
        }
      }
      for (uint32_t back = 0; back < batch;) {
        const uint32_t n = channel.completed.ConsumeAll([&](MeshWork* work) {
          misplaced += work != &works[back] ? 1 : 0;
          back++;
        });
        if (n == 0) {
          CpuRelax();
        }
      }
    };
    for (uint32_t round = 0; round < warmup; round++) {
      round_trip(round);
    }
    const auto begin = std::chrono::steady_clock::now();
    for (uint32_t round = warmup; round < warmup + rounds; round++) {
      round_trip(round);
    }
    const auto end = std::chrono::steady_clock::now();
    const std::chrono::duration<double, std::nano> elapsed = end - begin;
    run.ns_per_round_trip = elapsed.count() / rounds;
    run.misplaced = misplaced;
  };

  if (!RunOnCpuPair(pair, caller, callee)) {
    return std::nullopt;
  }
  return run;
}

TEST(SpscRingPerfTest, RoundTrip) {
  if (CoverageRequested()) {
    GTEST_SKIP() << "gcov's counters in PushBack and ConsumeAll are shared by "
                    "both threads";
  }
  const std::vector<CpuPair> pairs = GetCpuPairs();
  if (pairs.empty()) {
    GTEST_SKIP() << "no two cpus to run on";
  }

  struct Case {
    uint32_t batch;
    uint32_t rounds;
  };
  const Case cases[] = {
      {.batch = 1, .rounds = 20000},           // a lone call
      {.batch = kFlushBatch, .rounds = 4000},  // an early flush
      {.batch = kDepth, .rounds = 4000},       // the whole ring
  };
  std::printf(
      "[mesh channel round trips, depth %u, %u channels in turn, median of "
      "%d runs]\n",
      kDepth, kChannels, kRuns);
  for (const CpuPair& pair : pairs) {
    for (const Case& c : cases) {
      std::array<RoundTripRun, kRuns> runs;
      for (RoundTripRun& run : runs) {
        const std::optional<RoundTripRun> measured =
            MeasureRoundTrip(pair, c.batch, c.rounds);
        if (!measured.has_value()) {
          GTEST_SKIP() << "cannot bind to cpus " << pair.first << " and "
                       << pair.second;
        }
        run = *measured;
        EXPECT_EQ(run.misplaced, 0) << pair.placement << ", batch " << c.batch;
      }
      std::ranges::sort(runs, {}, &RoundTripRun::ns_per_round_trip);
      const RoundTripRun& median = runs[kRuns / 2];
      std::printf(
          "  %-14s cpus %3d -> %3d, batch %3u : %7.1f ns per round trip, "
          "%6.1f ns per call\n",
          pair.placement, pair.first, pair.second, c.batch,
          median.ns_per_round_trip, median.ns_per_round_trip / c.batch);
    }
  }
}

}  // namespace blockcache
}  // namespace dingofs
