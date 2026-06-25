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

#include "cache/tools/cache_bench/stats.h"

#include <algorithm>

#include "common/const.h"

namespace dingofs {
namespace cache {

void StatsSnapshot::Add(uint64_t op_bytes, uint64_t latency_us, bool ok) {
  ++operations;
  if (ok) {
    ++succeeded;
    bytes += op_bytes;
  } else {
    ++failed;
  }

  total_latency_us += latency_us;
  max_latency_us = std::max(max_latency_us, latency_us);
  if (min_latency_us == 0 || latency_us < min_latency_us) {
    min_latency_us = latency_us;
  }
}

double StatsSnapshot::OpsPerSec(uint64_t interval_us) const {
  if (interval_us == 0) {
    return 0;
  }
  return operations / (interval_us / 1000000.0);
}

double StatsSnapshot::MiBPerSec(uint64_t interval_us) const {
  if (interval_us == 0) {
    return 0;
  }
  return bytes / (interval_us / 1000000.0) / kMiB;
}

uint64_t StatsSnapshot::AvgLatencyUs() const {
  if (operations == 0) {
    return 0;
  }
  return total_latency_us / operations;
}

void Stats::Record(uint64_t op_bytes, uint64_t latency_us, bool ok) {
  std::lock_guard<std::mutex> lock(mutex_);
  interval_.Add(op_bytes, latency_us, ok);
  total_.Add(op_bytes, latency_us, ok);
}

StatsSnapshot Stats::TakeInterval() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto snapshot = interval_;
  interval_ = StatsSnapshot();
  return snapshot;
}

StatsSnapshot Stats::Total() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_;
}

}  // namespace cache
}  // namespace dingofs
