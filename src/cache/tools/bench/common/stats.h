/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_STATS_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_STATS_H_

#include <bthread/mutex.h>

#include <array>
#include <cstdint>
#include <memory>
#include <vector>

#include "cache/tools/bench/common/histogram.h"

namespace dingofs {
namespace cache {
namespace bench {

// Coarse latency distribution for the summary bar chart (last bucket = tail).
constexpr size_t kDistBuckets = 8;
extern const uint64_t kDistThresholdsUs[kDistBuckets];
extern const char* const kDistLabels[kDistBuckets];

struct Aggregated {
  uint64_t ops{0};
  uint64_t succeeded{0};
  uint64_t failed{0};
  uint64_t bytes{0};
  LatencyHistogram hist;
  std::array<uint64_t, kDistBuckets> dist{};
};

// Per-worker stats sharded by worker id: each worker is the only writer to its
// slot, so the only contention is that worker's own completions -- never a
// single global lock. Latencies recorded in microseconds.
class Stats {
 public:
  explicit Stats(uint32_t workers);

  void Record(uint32_t worker, uint64_t op_bytes, uint64_t latency_us, bool ok);
  Aggregated Aggregate() const;
  void Reset();

 private:
  struct alignas(64) Slot {
    mutable bthread::Mutex mutex;
    uint64_t ops{0};
    uint64_t succeeded{0};
    uint64_t failed{0};
    uint64_t bytes{0};
    LatencyHistogram hist;
    std::array<uint64_t, kDistBuckets> dist{};
  };

  std::vector<std::unique_ptr<Slot>> slots_;
};

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_STATS_H_
