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

#include "cache/tools/bench/common/histogram.h"

#include <algorithm>
#include <cmath>

namespace dingofs {
namespace cache {
namespace bench {

size_t LatencyHistogram::IndexOf(uint64_t v) {
  if (v < kSub) {
    return static_cast<size_t>(v);
  }
  const int octave = 63 - __builtin_clzll(v);
  const int shift = octave - kSubBits;
  const uint64_t sub = v >> shift;
  const size_t index =
      (static_cast<size_t>(shift + 1) * kSub) + static_cast<size_t>(sub - kSub);
  return std::min(index, kBucketCount - 1);
}

uint64_t LatencyHistogram::LowerBoundOf(size_t index) {
  if (index < kSub) {
    return index;
  }
  const size_t shift = (index / kSub) - 1;
  const uint64_t sub = index % kSub;
  return (sub + kSub) << shift;
}

uint64_t LatencyHistogram::UpperBoundOf(size_t index) {
  const uint64_t width = index < kSub ? 1 : (1ULL << ((index / kSub) - 1));
  return LowerBoundOf(index) + width - 1;
}

void LatencyHistogram::Add(uint64_t v) {
  if (count_ == 0 || v < min_) {
    min_ = v;
  }
  max_ = std::max(max_, v);
  ++count_;
  sum_ += v;
  ++buckets_[IndexOf(v)];
}

void LatencyHistogram::Merge(const LatencyHistogram& other) {
  if (other.count_ == 0) {
    return;
  }
  if (count_ == 0 || other.min_ < min_) {
    min_ = other.min_;
  }
  max_ = std::max(max_, other.max_);
  count_ += other.count_;
  sum_ += other.sum_;
  for (size_t i = 0; i < kBucketCount; ++i) {
    buckets_[i] += other.buckets_[i];
  }
}

void LatencyHistogram::Clear() {
  buckets_.fill(0);
  count_ = 0;
  sum_ = 0;
  min_ = 0;
  max_ = 0;
}

uint64_t LatencyHistogram::Percentile(double p) const {
  if (count_ == 0) {
    return 0;
  }
  p = std::min(std::max(p, 0.0), 1.0);
  uint64_t rank = static_cast<uint64_t>(std::ceil(p * count_));
  if (rank == 0) {
    rank = 1;
  }
  rank = std::min(rank, count_);

  uint64_t cumulative = 0;
  for (size_t i = 0; i < kBucketCount; ++i) {
    cumulative += buckets_[i];
    if (cumulative >= rank) {
      return std::min(UpperBoundOf(i), max_);
    }
  }
  return max_;
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
