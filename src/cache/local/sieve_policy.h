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

#ifndef DINGOFS_SRC_CACHE_LOCAL_SIEVE_POLICY_H_
#define DINGOFS_SRC_CACHE_LOCAL_SIEVE_POLICY_H_

#include <cstdint>

#include "cache/local/cache_entry.h"
#include "cache/local/cache_policy.h"

namespace dingofs {
namespace cache {

// SIEVE (NSDI'24): one FIFO list, one "hand", and a single visited bit per
// entry. A read hit only sets the visited bit -- no list movement -- which is
// what makes it both scan-resistant (a one-hit block is never re-visited, so
// the hand evicts it on its next pass) and cheap under concurrency. New entries
// enter at the newest end with visited=0; the hand sweeps oldest->newest,
// clearing visited bits and evicting the first unvisited entry, and it retains
// its position across evictions (this is what distinguishes SIEVE from CLOCK).
class SievePolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override;
  void OnAccess(CacheEntry* entry) override;
  void OnErase(CacheEntry* entry) override;
  void Evict(uint64_t want_bytes, uint64_t want_files,
             CacheVictims* victims) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    CacheVictims* victims) override;

 private:
  static constexpr uint8_t kVisited = 0x1;

  // Move the hand off `entry` if it points there, so `entry` can be unlinked.
  void AdvanceHandPast(CacheEntry* entry);

  CacheList list_;
  CacheEntry* hand_{nullptr};  // next eviction candidate; nullptr => start oldest
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_SIEVE_POLICY_H_
