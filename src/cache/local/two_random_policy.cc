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

#include "cache/local/two_random_policy.h"

#include <atomic>
#include <random>

namespace dingofs {
namespace cache {

static uint64_t NextRandom() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  return rng();
}

void TwoRandomPolicy::OnInsert(CacheEntry* entry) {
  entry->aux = static_cast<uint32_t>(entries_.size());
  entries_.push_back(entry);
}

void TwoRandomPolicy::RemoveEntry(CacheEntry* entry) {
  uint32_t idx = entry->aux;
  CacheEntry* last = entries_.back();
  entries_[idx] = last;
  last->aux = idx;
  entries_.pop_back();
}

void TwoRandomPolicy::OnErase(CacheEntry* entry) { RemoveEntry(entry); }

void TwoRandomPolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                            CacheVictims* victims) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  while ((freed_bytes < want_bytes || freed_files < want_files) &&
         !entries_.empty()) {
    size_t n = entries_.size();
    CacheEntry* victim;
    if (n == 1) {
      victim = entries_[0];
    } else {
      // two distinct random samples; evict the one with the older access time
      size_t i = NextRandom() % n;
      size_t j = NextRandom() % (n - 1);
      if (j >= i) {
        j++;
      }
      CacheEntry* a = entries_[i];
      CacheEntry* b = entries_[j];
      victim = (a->atime.load(std::memory_order_relaxed) <=
                b->atime.load(std::memory_order_relaxed))
                   ? a
                   : b;
    }
    RemoveEntry(victim);
    freed_bytes += victim->size;
    freed_files++;
    victims->push_back(victim);
  }
}

void TwoRandomPolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                                   uint64_t budget, CacheVictims* victims) {
  uint64_t checked = 0;
  size_t i = 0;
  while (i < entries_.size() && checked < budget) {
    checked++;
    CacheEntry* e = entries_[i];
    if (e->atime.load(std::memory_order_relaxed) + expire_sec <= now_sec) {
      victims->push_back(e);
      RemoveEntry(e);  // swap-removes: recheck the element now at index i
    } else {
      i++;
    }
  }
}

}  // namespace cache
}  // namespace dingofs
