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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_HISTOGRAM_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_HISTOGRAM_H_

#include <array>
#include <cstddef>
#include <cstdint>

namespace dingofs {
namespace cache {
namespace bench {

// Log-linear latency histogram (HdrHistogram-style): values below 64 keep 1-unit
// resolution, larger values keep 6 significant bits per power-of-two octave
// (~1.5% error). Small fixed array -> per-slot instances update lock-free on the
// hot path and merge at report time. Unit is whatever the caller records (us/ns).
class LatencyHistogram {
 public:
  void Add(uint64_t v);
  void Merge(const LatencyHistogram& other);
  void Clear();

  uint64_t Count() const { return count_; }
  uint64_t Sum() const { return sum_; }
  uint64_t Min() const { return count_ == 0 ? 0 : min_; }
  uint64_t Max() const { return max_; }
  uint64_t Mean() const { return count_ == 0 ? 0 : sum_ / count_; }
  uint64_t Percentile(double p) const;  // p in (0, 1]

 private:
  static constexpr int kSubBits = 6;
  static constexpr uint64_t kSub = 1ULL << kSubBits;  // 64
  static constexpr size_t kBucketCount = 2048;

  static size_t IndexOf(uint64_t v);
  static uint64_t LowerBoundOf(size_t index);
  static uint64_t UpperBoundOf(size_t index);

  std::array<uint64_t, kBucketCount> buckets_{};
  uint64_t count_{0};
  uint64_t sum_{0};
  uint64_t min_{0};
  uint64_t max_{0};
};

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_HISTOGRAM_H_
