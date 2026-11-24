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
#include <string>

namespace dingofs {
namespace blockcache {

void ConsistentHash::Add(std::string_view id, uint64_t weight) {
  CHECK(!final_) << "ring already finalized";
  CHECK_GT(weight, 0U) << "member weight must be positive";

  ids_.emplace_back(id);
  weights_.push_back(weight);
}

void ConsistentHash::Finalize() {
  CHECK(!final_) << "ring already finalized";

  const std::vector<uint64_t> counts = CalcPoints(weights_);
  for (uint32_t index = 0; index < ids_.size(); ++index) {
    // each hash gives two points: its low and its high 32 bits
    for (uint64_t i = 0; i < counts[index]; i += 2) {
      const uint64_t h = HashStr(ids_[index] + "-" + std::to_string(i));
      ring_.emplace_back(static_cast<uint32_t>(h), index);
      if (i + 1 < counts[index]) {
        ring_.emplace_back(static_cast<uint32_t>(h >> 32), index);
      }
    }
  }

  std::ranges::sort(ring_);
  final_ = true;
}

std::vector<uint64_t> ConsistentHash::CalcPoints(
    const std::vector<uint64_t>& weights) {
  if (weights.empty()) {
    return {};
  }

  const double lightest = static_cast<double>(std::ranges::min(weights));

  std::vector<double> wanted;
  wanted.reserve(weights.size());
  double total = 0;
  for (const uint64_t weight : weights) {
    wanted.push_back(kLightestPoints *
                     (static_cast<double>(weight) / lightest));
    total += wanted.back();
  }

  const double room = static_cast<double>(kMaxPoints - weights.size());
  const double scale = std::min(1.0, room / total);
  std::vector<uint64_t> counts;
  counts.reserve(weights.size());
  for (const double w : wanted) {
    counts.push_back(std::max<uint64_t>(1, static_cast<uint64_t>(w * scale)));
  }
  return counts;
}

std::string_view ConsistentHash::GetMember(uint64_t key) const {
  CHECK(final_) << "lookup before Finalize()";
  CHECK(!ring_.empty()) << "lookup on an empty ring";

  const auto point = static_cast<uint32_t>(HashInt(key));
  auto it = std::ranges::lower_bound(ring_, point, std::ranges::less{},
                                     &std::pair<uint32_t, uint32_t>::first);
  if (it == ring_.end()) {
    it = ring_.begin();
  }
  return ids_[it->second];
}

}  // namespace blockcache
}  // namespace dingofs
