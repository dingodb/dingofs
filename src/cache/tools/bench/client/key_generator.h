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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_KEY_GENERATOR_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_KEY_GENERATOR_H_

#include <atomic>
#include <cstdint>
#include <random>

#include "cache/tools/bench/client/options.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

// Draws BlockKeys over a key space of `keyspace` distinct blocks:
//   seq     -> each key once (preload); uniform -> independent draws;
//   zipf    -> YCSB-style skew with the hot set scrambled across shards/nodes.
// Zipf constants are precomputed once; each generator passes its own RNG.
class KeyGenerator {
 public:
  KeyGenerator(KeyDist dist, uint64_t keyspace, double zipf_theta,
               uint32_t block_size);

  bool Next(std::mt19937_64& rng, BlockKey* key);  // false: seq exhausted

 private:
  uint64_t NextId(std::mt19937_64& rng, bool* exhausted);
  BlockKey IdToKey(uint64_t id) const;
  static double Zeta(uint64_t n, double theta);
  static uint64_t Scramble(uint64_t x);

  const KeyDist dist_;
  const uint64_t keyspace_;
  const uint32_t block_size_;
  double theta_{0};
  double zetan_{0};
  double alpha_{0};
  double eta_{0};
  std::atomic<uint64_t> next_seq_{0};
};

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_KEY_GENERATOR_H_
