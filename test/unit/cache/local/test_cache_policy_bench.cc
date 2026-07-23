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
 * Created Date: 2026-07-23
 * Author: AI
 */

// Performance comparison of the eviction policies: hot-path throughput and
// hit ratio under a scan-polluted and a larger-than-cache workload. Prints a
// table; assertions only guard gross regressions / scan-resistance ordering.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "cache/local/cache_policy.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {
namespace {

constexpr uint32_t kBlk = 4096;
const std::vector<std::string> kPolicies = {"lru", "sieve", "s3fifo", "2random"};

// Minimal in-memory cache over a policy (no disk/threads), mirroring how
// DiskCacheManager drives the policy.
class Sim {
 public:
  Sim(const std::string& name, uint64_t cap_blocks)
      : policy_(NewEvictionPolicy(name)), cap_bytes_(cap_blocks * kBlk) {}

  bool Access(uint64_t id) {
    BlockHandle h(1, BlockKey(id, 0, kBlk));
    auto it = index_.find(h);
    if (it != index_.end()) {
      it->second.atime.store(tick_++, std::memory_order_relaxed);
      policy_->OnAccess(&it->second);
      return true;
    }
    auto [ins, ok] = index_.try_emplace(h);
    CacheEntry& e = ins->second;
    e.key = &ins->first;
    e.size = kBlk;
    e.atime.store(tick_++, std::memory_order_relaxed);
    policy_->OnInsert(&e);
    used_bytes_ += kBlk;
    while (used_bytes_ > cap_bytes_) {
      CacheVictims v;
      policy_->Evict(used_bytes_ - cap_bytes_, 0, &v);
      if (v.empty()) {
        break;
      }
      for (auto* victim : v) {
        used_bytes_ -= victim->size;
        BlockHandle key = *victim->key;
        index_.erase(key);
      }
    }
    return false;
  }

 private:
  EvictionPolicyUPtr policy_;
  CacheIndex index_;
  uint64_t cap_bytes_;
  uint64_t used_bytes_{0};
  uint32_t tick_{1};
};

double HotHitRatio(const std::string& name) {
  constexpr uint64_t kCap = 1000;   // blocks that fit
  constexpr uint64_t kHot = 900;    // hot working set (fits, re-read often)
  constexpr uint64_t kScan = 500;   // cold one-pass scan per epoch
  constexpr int kWarm = 3;          // build residency/frequency for the hot set
  constexpr int kEpochs = 6;
  Sim sim(name, kCap);
  for (int r = 0; r < kWarm; ++r) {
    for (uint64_t id = 0; id < kHot; ++id) {
      sim.Access(id);
    }
  }
  uint64_t hits = 0, total = 0, cold_id = 1'000'000;
  for (int epoch = 0; epoch < kEpochs; ++epoch) {
    for (uint64_t i = 0; i < kScan; ++i) {  // cold scan pollutes the cache
      sim.Access(cold_id++);
    }
    for (uint64_t id = 0; id < kHot; ++id) {  // re-read hot set, measure
      hits += sim.Access(id) ? 1 : 0;
      total++;
    }
  }
  return total ? static_cast<double>(hits) / total : 0.0;
}

double LoopHitRatio(const std::string& name) {
  // Working set 2x the cache, accessed uniformly at random.
  constexpr uint64_t kCap = 2000;
  constexpr uint64_t kSet = 4000;
  constexpr uint64_t kOps = 200000;
  Sim sim(name, kCap);
  std::mt19937_64 rng{12345};
  uint64_t hits = 0;
  for (uint64_t i = 0; i < kOps; ++i) {
    hits += sim.Access(rng() % kSet) ? 1 : 0;
  }
  return static_cast<double>(hits) / kOps;
}

}  // namespace

class PolicyBenchTest : public ::testing::Test {};

TEST_F(PolicyBenchTest, HitPathThroughput) {
  constexpr size_t kEntries = 100000;
  constexpr size_t kOps = 5'000'000;

  std::printf("\n[policy hit-path throughput, %zu ops over %zu entries]\n", kOps,
              kEntries);
  std::printf("  %-8s %12s %10s\n", "policy", "ns/op", "Mops/s");
  for (const auto& name : kPolicies) {
    std::vector<std::unique_ptr<BlockHandle>> handles;
    std::vector<std::unique_ptr<CacheEntry>> entries;
    auto policy = NewEvictionPolicy(name);
    handles.reserve(kEntries);
    entries.reserve(kEntries);
    for (size_t i = 0; i < kEntries; ++i) {
      handles.push_back(std::make_unique<BlockHandle>(1, BlockKey(i, 0, kBlk)));
      entries.push_back(std::make_unique<CacheEntry>());
      CacheEntry* e = entries.back().get();
      e->key = handles.back().get();
      e->size = kBlk;
      policy->OnInsert(e);
    }

    std::mt19937_64 rng{999};
    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kOps; ++i) {
      policy->OnAccess(entries[rng() % kEntries].get());
    }
    auto t1 = std::chrono::steady_clock::now();
    double ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    std::printf("  %-8s %12.2f %10.1f\n", name.c_str(), ns / kOps,
                kOps / (ns / 1e9) / 1e6);
  }
}

TEST_F(PolicyBenchTest, ScanResistanceHitRatio) {
  std::printf("\n[hot-set hit ratio under a cold scan / under a 2x working set]\n");
  std::printf("  %-8s %14s %16s\n", "policy", "scan hit%", "2x-set hit%");
  double lru_scan = 0, sieve_scan = 0, s3fifo_scan = 0;
  for (const auto& name : kPolicies) {
    double scan = HotHitRatio(name);
    double loop = LoopHitRatio(name);
    std::printf("  %-8s %13.1f%% %15.1f%%\n", name.c_str(), scan * 100,
                loop * 100);
    if (name == "lru") lru_scan = scan;
    if (name == "sieve") sieve_scan = scan;
    if (name == "s3fifo") s3fifo_scan = scan;
  }
  // The whole point: scan-resistant policies keep the hot set that LRU loses.
  EXPECT_GT(sieve_scan, lru_scan);
  EXPECT_GT(s3fifo_scan, lru_scan);
}

}  // namespace cache
}  // namespace dingofs
