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

#include "cache/local/cache_policy.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <string>

namespace dingofs {
namespace cache {

DEFINE_string(cache_eviction, "lru",
              "cache eviction policy: lru | 2random | s3fifo | sieve | none. "
              "Read once at cache startup; changing it requires a restart.");

static bool ValidateEviction(const char*, const std::string& value) {
  return value == "lru" || value == "2random" || value == "s3fifo" ||
         value == "sieve" || value == "none";
}
DEFINE_validator(cache_eviction, &ValidateEviction);

EvictionPolicyUPtr NewEvictionPolicy(const std::string& name) {
  const std::string& policy = name.empty() ? FLAGS_cache_eviction : name;
  if (policy == "2random") {
    return std::make_unique<TwoRandomPolicy>();
  }
  if (policy == "s3fifo") {
    return std::make_unique<S3FifoPolicy>();
  }
  if (policy == "sieve") {
    return std::make_unique<SievePolicy>();
  }
  if (policy == "none") {
    return std::make_unique<NonePolicy>();
  }
  if (policy != "lru") {
    LOG(WARNING) << "Unknown cache eviction policy '" << policy
                 << "', falling back to lru.";
  }
  return std::make_unique<LruPolicy>();
}

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
      RemoveEntry(e);  // swaps last into i -> recheck i
    } else {
      i++;
    }
  }
}

uint8_t S3FifoPolicy::Freq(const CacheEntry* e) {
  return e->meta.load(std::memory_order_relaxed) & kFreqMask;
}

bool S3FifoPolicy::InMain(const CacheEntry* e) {
  return (e->meta.load(std::memory_order_relaxed) & kInMain) != 0;
}

void S3FifoPolicy::OnInsert(CacheEntry* entry) {
  if (GhostContains(*entry->key)) {  // proven reuse -> straight to main
    GhostRemove(*entry->key);
    entry->meta.store(kInMain, std::memory_order_relaxed);
    main_.PushBack(entry);
    m_bytes_ += entry->size;
    m_count_++;
  } else {
    entry->meta.store(0, std::memory_order_relaxed);
    small_.PushBack(entry);
    s_bytes_ += entry->size;
    s_count_++;
  }
}

void S3FifoPolicy::OnAccess(CacheEntry* entry) {
  uint8_t meta = entry->meta.load(std::memory_order_relaxed);
  uint8_t freq = meta & kFreqMask;
  if (freq < kMaxFreq) {
    entry->meta.store((meta & ~kFreqMask) | (freq + 1),
                      std::memory_order_relaxed);
  }
}

void S3FifoPolicy::RemoveFromQueue(CacheEntry* entry) {
  CacheList::Remove(entry);
  if (InMain(entry)) {
    m_bytes_ -= entry->size;
    m_count_--;
  } else {
    s_bytes_ -= entry->size;
    s_count_--;
  }
}

void S3FifoPolicy::OnErase(CacheEntry* entry) { RemoveFromQueue(entry); }

CacheEntry* S3FifoPolicy::EvictSmallOne() {
  CacheEntry* t = small_.Oldest();
  CacheList::Remove(t);
  s_bytes_ -= t->size;
  s_count_--;
  if (Freq(t) >= 1) {  // used at least once -> promote, keep frequency
    t->meta.fetch_or(kInMain, std::memory_order_relaxed);
    main_.PushBack(t);
    m_bytes_ += t->size;
    m_count_++;
    return nullptr;
  }
  GhostAdd(*t->key);
  return t;
}

CacheEntry* S3FifoPolicy::EvictMainOne() {
  CacheEntry* t = main_.Oldest();
  uint8_t meta = t->meta.load(std::memory_order_relaxed);
  uint8_t freq = meta & kFreqMask;
  CacheList::Remove(t);
  if (freq >= 1) {  // second chance
    t->meta.store((meta & ~kFreqMask) | (freq - 1), std::memory_order_relaxed);
    main_.PushBack(t);
    return nullptr;
  }
  m_bytes_ -= t->size;
  m_count_--;
  return t;  // main evictions do not enter the ghost
}

CacheEntry* S3FifoPolicy::EvictStep() {
  uint64_t target_s =
      static_cast<uint64_t>((s_bytes_ + m_bytes_) * kSmallRatio);
  if (s_count_ > 0 && s_bytes_ >= target_s) {
    return EvictSmallOne();
  }
  if (m_count_ > 0) {
    return EvictMainOne();
  }
  if (s_count_ > 0) {
    return EvictSmallOne();
  }
  return nullptr;
}

void S3FifoPolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                         CacheVictims* victims) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  while ((freed_bytes < want_bytes || freed_files < want_files) &&
         (s_count_ > 0 || m_count_ > 0)) {
    CacheEntry* victim = EvictStep();
    if (victim != nullptr) {  // promotions/reinserts free no bytes
      freed_bytes += victim->size;
      freed_files++;
      victims->push_back(victim);
    }
  }
}

void S3FifoPolicy::ScanExpired(CacheList& list, uint32_t now_sec,
                               uint32_t expire_sec, uint64_t budget,
                               uint64_t* checked, CacheVictims* victims) {
  CacheEntry* cur = list.Oldest();
  while (cur != nullptr && *checked < budget) {
    CacheEntry* next = cur->next;
    (*checked)++;
    if (cur->atime.load(std::memory_order_relaxed) + expire_sec <= now_sec) {
      RemoveFromQueue(cur);
      victims->push_back(cur);
    }
    cur = (next == list.Sentinel()) ? nullptr : next;
  }
}

void S3FifoPolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                                uint64_t budget, CacheVictims* victims) {
  uint64_t checked = 0;
  ScanExpired(main_, now_sec, expire_sec, budget, &checked, victims);
  ScanExpired(small_, now_sec, expire_sec, budget, &checked, victims);
}

bool S3FifoPolicy::GhostContains(const BlockHandle& handle) const {
  return ghost_pos_.find(handle) != ghost_pos_.end();
}

void S3FifoPolicy::GhostAdd(const BlockHandle& handle) {
  if (ghost_pos_.find(handle) != ghost_pos_.end()) {
    return;
  }
  ghost_fifo_.push_back(handle);
  ghost_pos_.emplace(handle, std::prev(ghost_fifo_.end()));
  uint64_t cap = std::max<uint64_t>(s_count_ + m_count_, 16);  // self-tuning
  while (ghost_fifo_.size() > cap) {
    ghost_pos_.erase(ghost_fifo_.front());
    ghost_fifo_.pop_front();
  }
}

void S3FifoPolicy::GhostRemove(const BlockHandle& handle) {
  auto it = ghost_pos_.find(handle);
  if (it != ghost_pos_.end()) {
    ghost_fifo_.erase(it->second);
    ghost_pos_.erase(it);
  }
}

void SievePolicy::OnInsert(CacheEntry* entry) {
  entry->meta.store(0, std::memory_order_relaxed);
  list_.PushBack(entry);
}

void SievePolicy::OnAccess(CacheEntry* entry) {
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
      hand_ = list_.Oldest();
    }
    CacheEntry* cand = hand_;
    CacheEntry* next = cand->next;
    CacheEntry* after = (next == list_.Sentinel()) ? nullptr : next;

    if ((cand->meta.load(std::memory_order_relaxed) & kVisited) != 0) {
      cand->meta.fetch_and(static_cast<uint8_t>(~kVisited),
                           std::memory_order_relaxed);
      hand_ = after;
    } else {
      hand_ = after;
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
