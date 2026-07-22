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
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_ADMISSION_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_ADMISSION_H_

#include <gflags/gflags_declare.h>

#include <random>
#include <unordered_set>

#include "cache/blockcache/eviction_policy.h"

namespace dingofs {
namespace cache {

DECLARE_bool(cache_admit_second_hit);
DECLARE_uint32(cache_admit_write_budget_mbps);

// SSD write admission, the outermost filter in front of every cache-fill
// disk write (DiskCache::Cache). Two independent, default-off mechanisms:
//
//  1. second-hit doorkeeper (Akamai / Google LARC): a block is written only
//     on its second admission attempt within a rotating time window — one-hit
//     wonders never reach the disk at all
//  2. write budget (CacheLib DynamicRandomAP, simplified): a probability
//     adjusted every 60s so the observed cache-write rate converges to the
//     configured budget; no size penalty since blocks are uniformly ~4MB
//
// Explicit warmup always admits; stage/writeback never routes through here.
// Pure logic: no locks (callers serialize), no IO; flags are sampled by the
// caller into Config.
class AdmissionController {
 public:
  struct Config {
    bool second_hit;             // require a second touch within the window
    uint32_t write_budget_mbps;  // 0 = unlimited
  };

  // returns true if the block may be written to the cache disk
  bool Admit(const Config& config, uint64_t now_s, const CacheKey& key,
             BlockSource source, size_t size);

  void Reset();

  uint64_t Accepts() const { return accepts_; }
  uint64_t RejectsSecondHit() const { return rejects_second_hit_; }
  uint64_t RejectsWriteBudget() const { return rejects_write_budget_; }

 private:
  static constexpr uint64_t kDoorkeeperWindowS = 3600;
  static constexpr uint64_t kBudgetWindowS = 60;

  bool PassSecondHit(uint64_t now_s, const CacheKey& key);
  bool PassWriteBudget(const Config& config, uint64_t now_s, size_t size);

  // exact rotating doorkeeper instead of a bloom filter: entry count is
  // bounded by distinct first-touch blocks per hour (fingerprints only),
  // and exactness spares us tuning false-positive rates
  std::unordered_set<uint64_t> seen_[2];
  uint64_t seen_epoch_{0};

  double probability_{1.0};
  uint64_t budget_window_start_{0};
  uint64_t budget_window_bytes_{0};
  std::mt19937_64 rng_{std::random_device{}()};

  uint64_t accepts_{0};
  uint64_t rejects_second_hit_{0};
  uint64_t rejects_write_budget_{0};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_ADMISSION_H_
