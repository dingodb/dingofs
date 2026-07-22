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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_EVICTION_GUARD_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_EVICTION_GUARD_H_

#include <gflags/gflags_declare.h>

#include "cache/blockcache/eviction_policy.h"

namespace dingofs {
namespace cache {

DECLARE_uint32(warmup_protect_s);
DECLARE_double(warmup_protect_max_ratio);

// Soft protection lease for warmed blocks, composed into the eviction filter
// so neither the policy nor the manager grows protection branches.
//
// Protection is an ordering preference, never an exclusion (an unevictable
// class can wedge a cache — see Ceph's pinned-item bug). Three safeguards:
//   1. over the protected-bytes quota the filter passes through, so leases
//      age out in scan order (protected blocks evict oldest-first)
//   2. a round that freed nothing under pressure forces the next round to
//      ignore leases entirely
//   3. the free-space stop band still rejects as the final backstop
//
// Pure logic: no locks (callers serialize), no IO; flags are sampled by the
// caller into Config so every method is deterministic and unit-testable.
class EvictionGuard {
 public:
  struct Config {
    uint32_t protect_s;  // lease duration in seconds, 0 = disabled
    double max_ratio;    // protected-bytes quota as a fraction of capacity
  };

  explicit EvictionGuard(uint64_t capacity_bytes)
      : capacity_bytes_(capacity_bytes) {}

  // stamps a protection lease onto warmup blocks and accounts them
  void OnAdd(const Config& config, uint64_t now_s, CacheValue* value);

  // releases accounting when a block leaves the cache for any reason
  void OnRemove(const CacheValue& value);

  // wraps a base eviction filter with lease protection
  EvictionPolicy::FilterFunc Wrap(const Config& config, uint64_t now_s,
                                  EvictionPolicy::FilterFunc base);

  // outcome of an eviction round under capacity/free-space pressure: a
  // barren round arms safeguard #2
  void OnRoundEnd(bool under_pressure, uint64_t freed_bytes);

  void Reset();

  uint64_t ProtectedBytes() const { return protected_bytes_; }
  uint64_t ProtectSkips() const { return protect_skips_; }
  uint64_t ForceUnprotectRounds() const { return force_rounds_; }

 private:
  const uint64_t capacity_bytes_;
  uint64_t protected_bytes_{0};
  uint64_t protect_skips_{0};
  uint64_t force_rounds_{0};
  bool force_next_round_{false};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_EVICTION_GUARD_H_
