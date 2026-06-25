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

#ifndef DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_STATS_H_
#define DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_STATS_H_

#include <cstdint>
#include <mutex>

namespace dingofs {
namespace cache {

struct StatsSnapshot {
  uint64_t operations{0};
  uint64_t succeeded{0};
  uint64_t failed{0};
  uint64_t bytes{0};
  uint64_t total_latency_us{0};
  uint64_t min_latency_us{0};
  uint64_t max_latency_us{0};

  void Add(uint64_t op_bytes, uint64_t latency_us, bool ok);
  bool Empty() const { return operations == 0; }

  double OpsPerSec(uint64_t interval_us) const;
  double MiBPerSec(uint64_t interval_us) const;
  uint64_t AvgLatencyUs() const;
};

class Stats {
 public:
  void Record(uint64_t op_bytes, uint64_t latency_us, bool ok);

  StatsSnapshot TakeInterval();
  StatsSnapshot Total() const;

 private:
  mutable std::mutex mutex_;
  StatsSnapshot interval_;
  StatsSnapshot total_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_STATS_H_
