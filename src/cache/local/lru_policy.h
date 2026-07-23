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

#ifndef DINGOFS_SRC_CACHE_LOCAL_LRU_POLICY_H_
#define DINGOFS_SRC_CACHE_LOCAL_LRU_POLICY_H_

#include <cstdint>

#include "cache/local/cache_entry.h"
#include "cache/local/cache_policy.h"

namespace dingofs {
namespace cache {

// Plain (textbook) LRU: one intrusive list, move-to-newest on hit, evict from
// the oldest end. This is the simple, well-understood baseline; the scan
// resistance for AI-training workloads comes from the SIEVE / S3-FIFO policies.
class LruPolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override { list_.PushBack(entry); }
  void OnAccess(CacheEntry* entry) override { list_.MoveToNewest(entry); }
  void OnErase(CacheEntry* entry) override { CacheList::Remove(entry); }

  void Evict(uint64_t want_bytes, uint64_t want_files,
             CacheVictims* victims) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    CacheVictims* victims) override;

 private:
  CacheList list_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_LRU_POLICY_H_
