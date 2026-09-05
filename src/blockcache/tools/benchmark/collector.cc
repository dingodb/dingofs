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

#include "blockcache/tools/benchmark/collector.h"

#include <algorithm>

namespace dingofs {
namespace blockcache {

namespace {
constexpr uint64_t kMiB = 1ULL << 20;
}  // namespace

void Stat::Add(uint64_t bytes, uint64_t latency_ns) {
  max_latency_ns_ = std::max(max_latency_ns_, latency_ns);
  if (min_latency_ns_ == 0 || latency_ns < min_latency_ns_) {
    min_latency_ns_ = latency_ns;
  }

  count_++;
  total_bytes_ += bytes;
  total_latency_ns_ += latency_ns;
}

void Stat::Merge(const Stat& other) {
  max_latency_ns_ = std::max(max_latency_ns_, other.max_latency_ns_);
  if (other.min_latency_ns_ != 0 &&
      (min_latency_ns_ == 0 || other.min_latency_ns_ < min_latency_ns_)) {
    min_latency_ns_ = other.min_latency_ns_;
  }

  count_ += other.count_;
  total_bytes_ += other.total_bytes_;
  total_latency_ns_ += other.total_latency_ns_;
}

uint64_t Stat::IOPS(uint64_t interval_us) const {
  if (interval_us == 0) {
    return 0;
  }
  return count_ / (interval_us * 1.0 / 1e6);
}

uint64_t Stat::Bandwidth(uint64_t interval_us) const {
  if (interval_us == 0) {
    return 0;
  }
  return total_bytes_ * 1.0 / (interval_us * 1.0 / 1e6) / kMiB;
}

uint64_t Stat::AvgLat() const {
  if (count_ == 0) {
    return 0;
  }
  return total_latency_ns_ / count_;
}

uint64_t Stat::MaxLat() const { return max_latency_ns_; }

uint64_t Stat::MinLat() const { return min_latency_ns_; }

uint64_t Stat::Count() const { return count_; }

Collector::Collector(uint32_t slots) {
  slots_.reserve(slots);
  for (uint32_t i = 0; i < slots; i++) {
    slots_.push_back(std::make_unique<Slot>());
  }
}

void Collector::Drain(Stat* into) {
  for (auto& slot : slots_) {
    slot->Drain(into);
  }
}

}  // namespace blockcache
}  // namespace dingofs
