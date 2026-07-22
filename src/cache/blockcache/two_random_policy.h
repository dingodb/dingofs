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
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_TWO_RANDOM_POLICY_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_TWO_RANDOM_POLICY_H_

#include <random>
#include <string>
#include <unordered_map>

#include "cache/blockcache/eviction_policy.h"

namespace dingofs {
namespace cache {

// The JuiceFS-style "power of two choices" LRU approximation: a hit only
// refreshes atime (no list maintenance at all); eviction samples two random
// entries and evicts the one with the older atime. Smallest possible
// implementation and no hit-path reordering, but NOT scan-resistant — suited
// for workloads dominated by repeated random access without large one-pass
// sequential scans.
class TwoRandomPolicy final : public EvictionPolicy {
 public:
  void Add(const CacheKey& key, const CacheValue& value) override;
  bool Touch(const CacheKey& key, CacheValue* value) override;
  bool Exist(const CacheKey& key) const override;
  bool Get(const CacheKey& key, CacheValue* value) const override;
  bool Delete(const CacheKey& key, CacheValue* deleted) override;
  CacheItems Evict(FilterFunc filter) override;
  CacheItems Sweep(FilterFunc filter) override;

  size_t Size() const override;
  void Clear() override;

 private:
  struct Entry {
    CacheValue value;
    size_t index_pos;  // position in index_, kept in sync by swap-remove
  };

  using Map = std::unordered_map<std::string, Entry>;

  void RemoveEntry(Map::iterator iter);
  Map::iterator Sample();

  Map entries_;
  std::vector<Map::iterator> index_;  // for O(1) uniform sampling
  std::mt19937_64 rng_{std::random_device{}()};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_TWO_RANDOM_POLICY_H_
