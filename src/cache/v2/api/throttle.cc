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

#include "cache/v2/api/throttle.h"

namespace dingofs {
namespace cache {
namespace v2 {

InflightThrottle::InflightThrottle(unsigned shards, uint32_t max_inflight)
    : shards_(shards),
      max_inflight_(max_inflight),
      inflights_(std::make_unique<Inflight[]>(shards)) {}

bool InflightThrottle::Acquire(unsigned shard) {
  std::atomic<uint32_t>& count = inflights_[shard].count;
  if (count.fetch_add(1, std::memory_order_acq_rel) >= max_inflight_) {
    count.fetch_sub(1, std::memory_order_acq_rel);
    return false;
  }
  return true;
}

void InflightThrottle::Release(unsigned shard) {
  inflights_[shard].count.fetch_sub(1, std::memory_order_acq_rel);
}

uint32_t InflightThrottle::Inflights() const {
  uint32_t sum = 0;
  for (unsigned shard = 0; shard < shards_; ++shard) {
    sum += inflights_[shard].count.load(std::memory_order_acquire);
  }
  return sum;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
