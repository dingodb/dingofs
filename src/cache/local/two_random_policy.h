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

#ifndef DINGOFS_SRC_CACHE_LOCAL_TWO_RANDOM_POLICY_H_
#define DINGOFS_SRC_CACHE_LOCAL_TWO_RANDOM_POLICY_H_

#include <cstdint>
#include <vector>

#include "cache/local/cache_entry.h"
#include "cache/local/cache_policy.h"

namespace dingofs {
namespace cache {

// 2-random (JuiceFS's default): keep entries in a flat vector and, to evict,
// sample two at random and drop the one with the older access time. This is an
// O(1), low-overhead LRU approximation. It is NOT scan-resistant -- it is
// offered as a cheap baseline; SIEVE / S3-FIFO are the scan-resistant choices.
class TwoRandomPolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override;
  void OnAccess(CacheEntry* /*entry*/) override {}  // atime is set by the owner
  void OnErase(CacheEntry* entry) override;
  void Evict(uint64_t want_bytes, uint64_t want_files,
             CacheVictims* victims) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    CacheVictims* victims) override;

 private:
  // Swap-remove `entry` from the vector in O(1) using its stored index (aux).
  void RemoveEntry(CacheEntry* entry);

  std::vector<CacheEntry*> entries_;  // entry->aux is its index here
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_TWO_RANDOM_POLICY_H_
