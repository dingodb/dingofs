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
 * Created Date: 2026-06-03
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_LATENCY_TRACE_H_
#define DINGOFS_SRC_CACHE_COMMON_LATENCY_TRACE_H_

#include <butil/time.h>
#include <glog/logging.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <sstream>

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

inline uint64_t TraceNowUs() { return butil::cpuwide_time_us(); }

template <size_t N>
class LatencyTraceStats {
 public:
  LatencyTraceStats(const char* name, std::array<const char*, N> fields)
      : name_(name), fields_(fields) {}

  void Add(const std::array<uint64_t, N>& values) {
    if (!FLAGS_cache_trace_range_latency) {
      return;
    }

    uint64_t count = count_.fetch_add(1, std::memory_order_relaxed) + 1;
    for (size_t i = 0; i < N; ++i) {
      sums_[i].fetch_add(values[i], std::memory_order_relaxed);
      UpdateMax(maxes_[i], values[i]);
    }

    uint64_t every = FLAGS_cache_trace_range_latency_every;
    if (every == 0 || count % every != 0) {
      return;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1);
    oss << "range_latency_trace name=" << name_ << " count=" << count;
    for (size_t i = 0; i < N; ++i) {
      uint64_t sum = sums_[i].load(std::memory_order_relaxed);
      uint64_t max = maxes_[i].load(std::memory_order_relaxed);
      oss << ' ' << fields_[i] << "_avg_us="
          << (static_cast<double>(sum) / static_cast<double>(count)) << ' '
          << fields_[i] << "_max_us=" << max;
    }
    LOG(INFO) << oss.str();
  }

 private:
  static void UpdateMax(std::atomic<uint64_t>& current, uint64_t value) {
    uint64_t old = current.load(std::memory_order_relaxed);
    while (old < value &&
           !current.compare_exchange_weak(old, value, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
    }
  }

  const char* name_;
  std::array<const char*, N> fields_;
  std::atomic<uint64_t> count_{0};
  std::array<std::atomic<uint64_t>, N> sums_{};
  std::array<std::atomic<uint64_t>, N> maxes_{};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_LATENCY_TRACE_H_
