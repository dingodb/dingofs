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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_EVICTION_POLICY_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_EVICTION_POLICY_H_

#include <gflags/gflags_declare.h>

#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include "cache/blockcache/cache_store.h"
#include "cache/iutil/time_util.h"

namespace dingofs {
namespace cache {

DECLARE_string(cache_eviction_policy);
DECLARE_double(cache_s3fifo_small_ratio);

using CacheKey = BlockKey;

struct CacheValue {
  CacheValue() = default;
  CacheValue(size_t size, iutil::TimeSpec atime) : size(size), atime(atime) {}

  size_t size{0};
  iutil::TimeSpec atime;      // refreshed only by Touch (a real cache hit)
  uint8_t source{0};          // BlockSource, filled by admission/eviction guard
  uint8_t freq{0};            // access count, saturated at 3 (s3fifo)
  uint32_t protect_until{0};  // protection lease deadline in epoch seconds
};

struct CacheItem {
  CacheItem(CacheKey key, CacheValue value) : key(key), value(value) {}

  CacheKey key;
  CacheValue value;
};

using CacheItems = std::vector<CacheItem>;

enum class FilterStatus : uint8_t {
  kEvictIt,
  kSkip,
  kFinish,
};

// generic policy counters for metrics; policies without the concept report 0
struct EvictionPolicyStats {
  uint64_t small_bytes{0};    // s3fifo probationary queue bytes
  uint64_t ghost_entries{0};  // s3fifo ghost fingerprints
  uint64_t ghost_hits{0};     // readmissions via ghost
};

// Eviction policy over cached blocks: a pure in-memory data structure.
// Contract:
//  - no locks (callers serialize), no IO, no gflags (config via constructor)
//  - Touch() is the ONLY operation counted as an access: it refreshes atime
//    and promotes; Exist()/Get() are side-effect free so probing (IsCached,
//    dedup checks, prefetch fast-paths) never fakes hotness
//  - Evict() yields victims in policy order and terminates after at most one
//    pass over the entries even if the filter skips everything
//  - Sweep() visits entries without reordering or promoting: used by
//    time-based expiry, which must work identically for every policy
class EvictionPolicy {
 public:
  using FilterFunc = std::function<FilterStatus(const CacheValue& value)>;

  virtual ~EvictionPolicy() = default;

  virtual void Add(const CacheKey& key, const CacheValue& value) = 0;
  virtual bool Touch(const CacheKey& key, CacheValue* value) = 0;
  virtual bool Exist(const CacheKey& key) const = 0;
  virtual bool Get(const CacheKey& key, CacheValue* value) const = 0;
  virtual bool Delete(const CacheKey& key, CacheValue* deleted) = 0;
  virtual CacheItems Evict(FilterFunc filter) = 0;
  virtual CacheItems Sweep(FilterFunc filter) = 0;
  virtual size_t Size() const = 0;
  virtual void Clear() = 0;

  virtual EvictionPolicyStats GetStats() const { return {}; }
};

using EvictionPolicyUPtr = std::unique_ptr<EvictionPolicy>;

// name: "lru", "s3fifo" or "2random" (see FLAGS_cache_eviction_policy);
// dies on unknown name.
EvictionPolicyUPtr NewEvictionPolicy(std::string_view name,
                                     uint64_t capacity_bytes);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_EVICTION_POLICY_H_
