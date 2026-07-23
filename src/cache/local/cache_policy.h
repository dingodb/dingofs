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
 * Created Date: 2026-07-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_LOCAL_CACHE_POLICY_H_
#define DINGOFS_SRC_CACHE_LOCAL_CACHE_POLICY_H_

#include <cstdint>
#include <memory>
#include <string>

#include "cache/local/cache_entry.h"

namespace dingofs {
namespace cache {

// A swappable cache eviction discipline. It owns only the *ordering* of cached
// entries (an intrusive list, a sampling vector, ...) over CacheEntry objects
// whose lifetime and index belong to DiskCacheManager. Every method is invoked
// under the owning shard's mutex, so implementations need no internal locking.
//
// Staging (not-yet-uploaded writeback) blocks are pinned structurally: the
// manager never calls OnInsert for them, so they never enter the ordering and
// Evict/EvictExpired can never return them. The policy has no notion of staging.
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  // Link a freshly-cached entry into the ordering.
  virtual void OnInsert(CacheEntry* entry) = 0;
  // Record a read hit (set a bit / bump a counter / move to head).
  virtual void OnAccess(CacheEntry* entry) = 0;
  // Unlink an entry being explicitly deleted or demoted back to staging.
  virtual void OnErase(CacheEntry* entry) = 0;

  // Capacity eviction: select victims until at least want_bytes AND want_files
  // are released, or nothing evictable remains. Victims are unlinked from the
  // ordering and appended to `victims`; the caller erases them from the index,
  // updates usage, and unlinks the files asynchronously.
  virtual void Evict(uint64_t want_bytes, uint64_t want_files,
                     CacheVictims* victims) = 0;

  // TTL eviction: append up to `budget` entries whose access time is older than
  // now_sec - expire_sec, walking the ordering; skip fresh entries.
  virtual void EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                            uint64_t budget, CacheVictims* victims) = 0;
};

using EvictionPolicyUPtr = std::unique_ptr<EvictionPolicy>;

// Build the policy named by `name`; empty falls back to FLAGS_cache_eviction_policy.
// Valid names: "lru", "sieve", "s3fifo", "2random", "none".
EvictionPolicyUPtr NewEvictionPolicy(const std::string& name = "");

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_CACHE_POLICY_H_
