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

#include "cache/local/lru_policy.h"

#include <atomic>

namespace dingofs {
namespace cache {

void LruPolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                      CacheVictims* victims) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  CacheEntry* cur = list_.Oldest();
  while (cur != nullptr &&
         (freed_bytes < want_bytes || freed_files < want_files)) {
    CacheEntry* next = cur->next;
    CacheList::Remove(cur);
    freed_bytes += cur->size;
    freed_files++;
    victims->push_back(cur);
    cur = (next == list_.Sentinel()) ? nullptr : next;
  }
}

void LruPolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                             uint64_t budget, CacheVictims* victims) {
  uint64_t checked = 0;
  CacheEntry* cur = list_.Oldest();
  // Walk oldest-first, dropping expired entries and skipping fresh ones. We do
  // not stop at the first fresh entry: after a warm restart the loader inserts
  // entries in scan order rather than access order, so a fresh entry may sit
  // ahead of older ones. The budget bounds the per-tick scan cost.
  while (cur != nullptr && checked < budget) {
    CacheEntry* next = cur->next;
    checked++;
    if (cur->atime.load(std::memory_order_relaxed) + expire_sec <= now_sec) {
      CacheList::Remove(cur);
      victims->push_back(cur);
    }
    cur = (next == list_.Sentinel()) ? nullptr : next;
  }
}

}  // namespace cache
}  // namespace dingofs
