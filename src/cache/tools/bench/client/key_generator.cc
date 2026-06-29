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

#include "cache/tools/bench/client/key_generator.h"

#include <algorithm>
#include <cmath>

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

namespace {
constexpr uint64_t kBlocksPerChunk = 16;
}  // namespace

KeyGenerator::KeyGenerator(KeyDist dist, uint64_t keyspace, double zipf_theta,
                           uint32_t block_size)
    : dist_(dist), keyspace_(keyspace), block_size_(block_size) {
  if (dist_ == KeyDist::kZipf) {
    theta_ = zipf_theta;
    zetan_ = Zeta(keyspace_, theta_);
    const double zeta2 = Zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    eta_ = (1.0 - std::pow(2.0 / static_cast<double>(keyspace_), 1.0 - theta_)) /
           (1.0 - (zeta2 / zetan_));
  }
}

double KeyGenerator::Zeta(uint64_t n, double theta) {
  double sum = 0.0;
  for (uint64_t i = 1; i <= n; ++i) {
    sum += 1.0 / std::pow(static_cast<double>(i), theta);
  }
  return sum;
}

uint64_t KeyGenerator::Scramble(uint64_t x) {
  uint64_t h = 1469598103934665603ULL;  // FNV-1a 64-bit
  for (int i = 0; i < 8; ++i) {
    h ^= (x & 0xFF);
    h *= 1099511628211ULL;
    x >>= 8;
  }
  return h;
}

uint64_t KeyGenerator::NextId(std::mt19937_64& rng, bool* exhausted) {
  *exhausted = false;
  switch (dist_) {
    case KeyDist::kSeq: {
      const uint64_t id = next_seq_.fetch_add(1, std::memory_order_relaxed);
      if (id >= keyspace_) {
        *exhausted = true;
        return 0;
      }
      return id;
    }
    case KeyDist::kUniform: {
      std::uniform_int_distribution<uint64_t> dist(0, keyspace_ - 1);
      return dist(rng);
    }
    case KeyDist::kZipf: {
      std::uniform_real_distribution<double> dist(0.0, 1.0);
      const double u = dist(rng);
      const double uz = u * zetan_;
      uint64_t v = 0;
      if (uz < 1.0) {
        v = 0;
      } else if (uz < 1.0 + std::pow(0.5, theta_)) {
        v = 1;
      } else {
        v = static_cast<uint64_t>(static_cast<double>(keyspace_) *
                                  std::pow((eta_ * u) - eta_ + 1.0, alpha_));
      }
      v = std::min(v, keyspace_ - 1);
      return Scramble(v) % keyspace_;
    }
  }
  return 0;
}

BlockKey KeyGenerator::IdToKey(uint64_t id) const {
  const uint64_t slice_id = (id / kBlocksPerChunk) + 1;
  const auto index = static_cast<uint32_t>(id % kBlocksPerChunk);
  return BlockKey(slice_id, index, block_size_);
}

bool KeyGenerator::Next(std::mt19937_64& rng, BlockKey* key) {
  bool exhausted = false;
  const uint64_t id = NextId(rng, &exhausted);
  if (exhausted) {
    return false;
  }
  *key = IdToKey(id);
  return true;
}

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
