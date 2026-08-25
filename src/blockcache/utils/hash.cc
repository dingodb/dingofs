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

#include "blockcache/utils/hash.h"

#include <glog/logging.h>

#include <algorithm>
#include <functional>
#include <numeric>
#include <string>

namespace dingofs {
namespace blockcache {

void ConsistentHash::Add(uint32_t member, std::string_view id,
                         uint32_t weight) {
  CHECK(!final_) << "ring already finalized";
  CHECK_GT(weight, 0U) << "member weight must be positive";
  for (uint32_t i = 0; i < weight * kPointsPerWeight; i += 2) {
    const uint64_t h = Mix64(Fnv1a(std::string(id) + "-" + std::to_string(i)));
    ring_.emplace_back(static_cast<uint32_t>(h), member);
    ring_.emplace_back(static_cast<uint32_t>(h >> 32), member);
  }
}

void ConsistentHash::Finalize() {
  CHECK(!final_) << "ring already finalized";
  std::ranges::sort(ring_);
  final_ = true;
}

uint32_t ConsistentHash::MemberOf(uint64_t key) const {
  DCHECK(final_) << "lookup before Finalize()";
  DCHECK(!ring_.empty()) << "lookup on an empty ring";
  const auto point = static_cast<uint32_t>(Mix64(key));
  auto it = std::ranges::lower_bound(ring_, point, std::ranges::less{},
                                     &std::pair<uint32_t, uint32_t>::first);
  if (it == ring_.end()) {
    it = ring_.begin();
  }
  return it->second;
}

std::vector<uint32_t> NormalizeGcd(const std::vector<uint64_t>& values) {
  uint64_t gcd = 0;
  for (const uint64_t v : values) {
    gcd = std::gcd(gcd, v);
  }
  std::vector<uint32_t> weights;
  weights.reserve(values.size());
  for (const uint64_t v : values) {
    weights.push_back(gcd == 0 ? 1 : static_cast<uint32_t>(v / gcd));
  }
  return weights;
}

}  // namespace blockcache
}  // namespace dingofs
