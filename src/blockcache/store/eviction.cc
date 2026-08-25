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

#include "blockcache/store/eviction.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>

namespace dingofs {
namespace blockcache {

static uint64_t NextRandom() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  return rng();
}

DEFINE_string(cache_eviction, "lru",
              "eviction: lru | 2random | s3fifo | sieve | none");

static bool ValidateEviction(const char*, const std::string& value) {
  return value == "lru" || value == "2random" || value == "s3fifo" ||
         value == "sieve" || value == "none";
}
DEFINE_validator(cache_eviction, &ValidateEviction);

template <typename RemoveFn>
static void ScanExpiredList(EntryList& list, uint32_t now_sec,
                            uint32_t expire_sec, uint64_t budget,
                            uint64_t* checked, Evicted* to_del,
                            RemoveFn remove) {
  CacheEntry* cur = list.Oldest();
  while (cur != nullptr && *checked < budget) {
    CacheEntry* next = list.Next(cur);
    ++*checked;
    if (cur->atime + expire_sec <= now_sec) {
      remove(cur);
      to_del->push_back(cur);
    }
    cur = next;
  }
}

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
                      Evicted* to_del) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  CacheEntry* cur = list_.Oldest();
  while (cur != nullptr &&
         (freed_bytes < want_bytes || freed_files < want_files)) {
    CacheEntry* next = list_.Next(cur);
    EntryList::Remove(cur);
    freed_bytes += cur->size;
    freed_files++;
    to_del->push_back(cur);
    cur = next;
  }
}

void LruPolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                             uint64_t budget, Evicted* to_del) {
  uint64_t checked = 0;
  ScanExpiredList(list_, now_sec, expire_sec, budget, &checked, to_del,
                  [](CacheEntry* e) { EntryList::Remove(e); });
}

void TwoRandomPolicy::OnInsert(CacheEntry* entry) {
  entry->aux = static_cast<uint32_t>(entries_.size());
  entries_.push_back(entry);
}

void TwoRandomPolicy::OnErase(CacheEntry* entry) { RemoveEntry(entry); }

void TwoRandomPolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                            Evicted* to_del) {
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
      victim = (a->atime <= b->atime) ? a : b;
    }
    RemoveEntry(victim);
    freed_bytes += victim->size;
    freed_files++;
    to_del->push_back(victim);
  }
}

void TwoRandomPolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                                   uint64_t budget, Evicted* to_del) {
  uint64_t checked = 0;
  size_t i = 0;
  while (i < entries_.size() && checked < budget) {
    checked++;
    CacheEntry* e = entries_[i];
    if (e->atime + expire_sec <= now_sec) {
      to_del->push_back(e);
      RemoveEntry(e);
    } else {
      i++;
    }
  }
}

void TwoRandomPolicy::RemoveEntry(CacheEntry* entry) {
  uint32_t idx = entry->aux;
  CacheEntry* last = entries_.back();
  entries_[idx] = last;
  last->aux = idx;
  entries_.pop_back();
}

void S3FifoPolicy::OnInsert(CacheEntry* entry) {
  if (GhostContains(*entry->key)) {
    GhostRemove(*entry->key);
    entry->flags = kInMain;
    main_.PushBack(entry);
    main_bytes_ += entry->size;
    main_count_++;
  } else {
    entry->flags = 0;
    small_.PushBack(entry);
    small_bytes_ += entry->size;
    small_count_++;
  }
}

void S3FifoPolicy::OnAccess(CacheEntry* entry) {
  const uint8_t freq = Freq(entry);
  if (freq < kMaxFreq) {
    entry->flags = (entry->flags & ~kFreqMask) | (freq + 1);
  }
}

void S3FifoPolicy::OnErase(CacheEntry* entry) { RemoveFromQueue(entry); }

void S3FifoPolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                         Evicted* to_del) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  while ((freed_bytes < want_bytes || freed_files < want_files) &&
         (small_count_ > 0 || main_count_ > 0)) {
    CacheEntry* victim = EvictStep();
    if (victim != nullptr) {
      freed_bytes += victim->size;
      freed_files++;
      to_del->push_back(victim);
    }
  }
}

void S3FifoPolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                                uint64_t budget, Evicted* to_del) {
  uint64_t checked = 0;
  const auto remove = [this](CacheEntry* e) { RemoveFromQueue(e); };
  ScanExpiredList(main_, now_sec, expire_sec, budget, &checked, to_del, remove);
  ScanExpiredList(small_, now_sec, expire_sec, budget, &checked, to_del,
                  remove);
}

CacheEntry* S3FifoPolicy::EvictStep() {
  uint64_t target_small =
      static_cast<uint64_t>((small_bytes_ + main_bytes_) * kSmallRatio);
  if (small_count_ > 0 && small_bytes_ >= target_small) {
    return EvictSmallOne();
  }
  if (main_count_ > 0) {
    return EvictMainOne();
  }
  if (small_count_ > 0) {
    return EvictSmallOne();
  }
  return nullptr;
}

CacheEntry* S3FifoPolicy::EvictSmallOne() {
  CacheEntry* oldest = small_.Oldest();
  EntryList::Remove(oldest);
  small_bytes_ -= oldest->size;
  small_count_--;
  if (Freq(oldest) >= 1) {
    oldest->flags |= kInMain;
    main_.PushBack(oldest);
    main_bytes_ += oldest->size;
    main_count_++;
    return nullptr;
  }
  GhostAdd(*oldest->key);
  return oldest;
}

CacheEntry* S3FifoPolicy::EvictMainOne() {
  CacheEntry* oldest = main_.Oldest();
  const uint8_t freq = Freq(oldest);
  EntryList::Remove(oldest);
  if (freq >= 1) {
    oldest->flags = (oldest->flags & ~kFreqMask) | (freq - 1);
    main_.PushBack(oldest);
    return nullptr;
  }
  main_bytes_ -= oldest->size;
  main_count_--;
  return oldest;
}

void S3FifoPolicy::RemoveFromQueue(CacheEntry* entry) {
  EntryList::Remove(entry);
  if (InMain(entry)) {
    main_bytes_ -= entry->size;
    main_count_--;
  } else {
    small_bytes_ -= entry->size;
    small_count_--;
  }
}

bool S3FifoPolicy::GhostContains(BlockHandle handle) const {
  return ghost_pos_.find(handle) != ghost_pos_.end();
}

void S3FifoPolicy::GhostAdd(BlockHandle handle) {
  if (ghost_pos_.find(handle) != ghost_pos_.end()) {
    return;
  }
  ghost_fifo_.push_back(handle);
  ghost_pos_.emplace(handle, std::prev(ghost_fifo_.end()));
  uint64_t cap = std::max<uint64_t>(small_count_ + main_count_, 16);
  while (ghost_fifo_.size() > cap) {
    ghost_pos_.erase(ghost_fifo_.front());
    ghost_fifo_.pop_front();
  }
}

void S3FifoPolicy::GhostRemove(BlockHandle handle) {
  auto it = ghost_pos_.find(handle);
  if (it != ghost_pos_.end()) {
    ghost_fifo_.erase(it->second);
    ghost_pos_.erase(it);
  }
}

void SievePolicy::OnInsert(CacheEntry* entry) {
  entry->flags = 0;
  list_.PushBack(entry);
}

void SievePolicy::OnErase(CacheEntry* entry) {
  AdvanceHandPast(entry);
  EntryList::Remove(entry);
}

void SievePolicy::Evict(uint64_t want_bytes, uint64_t want_files,
                        Evicted* to_del) {
  uint64_t freed_bytes = 0;
  uint64_t freed_files = 0;
  while ((freed_bytes < want_bytes || freed_files < want_files) &&
         !list_.empty()) {
    if (hand_ == nullptr) {
      hand_ = list_.Oldest();
    }
    CacheEntry* candidate = hand_;
    CacheEntry* after = list_.Next(candidate);

    if ((candidate->flags & kVisited) != 0) {
      candidate->flags &= static_cast<uint8_t>(~kVisited);
      hand_ = after;
    } else {
      hand_ = after;
      EntryList::Remove(candidate);
      freed_bytes += candidate->size;
      freed_files++;
      to_del->push_back(candidate);
    }
  }
}

void SievePolicy::EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                               uint64_t budget, Evicted* to_del) {
  uint64_t checked = 0;
  ScanExpiredList(list_, now_sec, expire_sec, budget, &checked, to_del,
                  [this](CacheEntry* e) {
                    AdvanceHandPast(e);
                    EntryList::Remove(e);
                  });
}

void SievePolicy::AdvanceHandPast(CacheEntry* entry) {
  if (hand_ == entry) {
    hand_ = list_.Next(entry);
  }
}

}  // namespace blockcache
}  // namespace dingofs
