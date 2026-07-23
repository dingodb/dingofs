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

#include "cache/local/s3fifo_policy.h"

#include <algorithm>
#include <atomic>

namespace dingofs {
namespace cache {

uint8_t S3FifoPolicy::Freq(const CacheEntry* e) {
  return e->meta.load(std::memory_order_relaxed) & kFreqMask;
}

bool S3FifoPolicy::InMain(const CacheEntry* e) {
  return (e->meta.load(std::memory_order_relaxed) & kInMain) != 0;
}

void S3FifoPolicy::OnInsert(CacheEntry* entry) {
  if (GhostContains(*entry->key)) {
    // Proven reuse: admit straight to the main queue (fresh frequency).
    GhostRemove(*entry->key);
    entry->meta.store(kInMain, std::memory_order_relaxed);
    main_.PushBack(entry);
    m_bytes_ += entry->size;
    m_count_++;
  } else {
    entry->meta.store(0, std::memory_order_relaxed);  // freq 0, in small
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

void S3FifoPolicy::OnErase(CacheEntry* entry) {
  // Explicit delete or demote-to-staging: does NOT enter the ghost queue.
  RemoveFromQueue(entry);
}

CacheEntry* S3FifoPolicy::EvictSmallOne() {
  CacheEntry* t = small_.Oldest();
  CacheList::Remove(t);
  s_bytes_ -= t->size;
  s_count_--;

  if (Freq(t) >= 1) {  // used at least once: promote to main, keep frequency
    t->meta.fetch_or(kInMain, std::memory_order_relaxed);
    main_.PushBack(t);
    m_bytes_ += t->size;
    m_count_++;
    return nullptr;
  }
  // one-hit-wonder: evict and remember its key so a quick re-access re-admits it
  GhostAdd(*t->key);
  return t;
}

CacheEntry* S3FifoPolicy::EvictMainOne() {
  CacheEntry* t = main_.Oldest();
  uint8_t meta = t->meta.load(std::memory_order_relaxed);
  uint8_t freq = meta & kFreqMask;
  if (freq >= 1) {  // second chance: reinsert with decremented frequency
    CacheList::Remove(t);
    t->meta.store((meta & ~kFreqMask) | (freq - 1), std::memory_order_relaxed);
    main_.PushBack(t);
    return nullptr;
  }
  CacheList::Remove(t);
  m_bytes_ -= t->size;
  m_count_--;
  return t;  // evicted; main-queue evictions do not enter the ghost queue
}

CacheEntry* S3FifoPolicy::EvictStep() {
  uint64_t target_s = static_cast<uint64_t>((s_bytes_ + m_bytes_) * kSmallRatio);
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
    if (victim != nullptr) {
      freed_bytes += victim->size;
      freed_files++;
      victims->push_back(victim);
    }
  }
}

void S3FifoPolicy::ScanExpired(CacheList& list, bool /*in_main*/,
                               uint32_t now_sec, uint32_t expire_sec,
                               uint64_t budget, uint64_t* checked,
                               CacheVictims* victims) {
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
  // Insertion-ordered like SIEVE, so no early exit; scan both queues to budget.
  uint64_t checked = 0;
  ScanExpired(main_, true, now_sec, expire_sec, budget, &checked, victims);
  ScanExpired(small_, false, now_sec, expire_sec, budget, &checked, victims);
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

  // Self-tuning bound: keep at most one ghost key per live object.
  uint64_t cap = std::max<uint64_t>(s_count_ + m_count_, 16);
  while (ghost_fifo_.size() > cap) {
    ghost_pos_.erase(ghost_fifo_.front());
    ghost_fifo_.pop_front();
  }
}

void S3FifoPolicy::GhostRemove(const BlockHandle& handle) {
  auto it = ghost_pos_.find(handle);
  if (it == ghost_pos_.end()) {
    return;
  }
  ghost_fifo_.erase(it->second);
  ghost_pos_.erase(it);
}

}  // namespace cache
}  // namespace dingofs
