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

#include "cache/local/sieve_policy.h"

#include <atomic>

namespace dingofs {
namespace cache {

void SievePolicy::OnInsert(CacheEntry* entry) {
  entry->meta.store(0, std::memory_order_relaxed);  // visited = 0
  list_.PushBack(entry);
}

void SievePolicy::OnAccess(CacheEntry* entry) {
  // Lazy promotion: just set the visited bit, no list surgery.
  entry->meta.fetch_or(kVisited, std::memory_order_relaxed);
}

void SievePolicy::AdvanceHandPast(CacheEntry* entry) {
  if (hand_ == entry) {
    CacheEntry* next = entry->next;
    hand_ = (next == list_.Sentinel()) ? nullptr : next;
  }
}

void SievePolicy::OnErase(CacheEntry* entry) {
  AdvanceHandPast(entry);
  CacheList::Remove(entry);
}

void SievePolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                        CacheVictims* victims) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  while ((freed_bytes < want_bytes || freed_files < want_files) &&
         !list_.Empty()) {
    if (hand_ == nullptr) {
      hand_ = list_.Oldest();  // wrap to the oldest end
    }

    CacheEntry* cand = hand_;
    CacheEntry* next = cand->next;
    CacheEntry* after = (next == list_.Sentinel()) ? nullptr : next;

    if ((cand->meta.load(std::memory_order_relaxed) & kVisited) != 0) {
      // Second chance: clear the bit and move on (no eviction).
      cand->meta.fetch_and(static_cast<uint8_t>(~kVisited),
                           std::memory_order_relaxed);
      hand_ = after;
    } else {
      hand_ = after;  // hand retains its position past the evicted entry
      CacheList::Remove(cand);
      freed_bytes += cand->size;
      freed_files++;
      victims->push_back(cand);
    }
  }
}

void SievePolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                               uint64_t budget, CacheVictims* victims) {
  uint64_t checked = 0;
  CacheEntry* cur = list_.Oldest();
  // SIEVE is insertion-ordered (a hit refreshes atime but does not move the
  // node), so we cannot early-exit at the first fresh entry; scan up to budget.
  while (cur != nullptr && checked < budget) {
    CacheEntry* next = cur->next;
    checked++;
    if (cur->atime.load(std::memory_order_relaxed) + expire_sec <= now_sec) {
      AdvanceHandPast(cur);
      CacheList::Remove(cur);
      victims->push_back(cur);
    }
    cur = (next == list_.Sentinel()) ? nullptr : next;
  }
}

}  // namespace cache
}  // namespace dingofs
