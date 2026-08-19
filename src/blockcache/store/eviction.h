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

#ifndef DINGOFS_BLOCKCACHE_STORE_EVICTION_H_
#define DINGOFS_BLOCKCACHE_STORE_EVICTION_H_

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "blockcache/common/block_handle.h"

namespace dingofs {
namespace blockcache {

struct CacheEntry {
  const BlockHandle* key{nullptr};
  uint32_t size{0};
  uint32_t atime{0};
  bool staged{false};
  uint8_t flags{0};
  uint32_t aux{0};

  CacheEntry* prev{nullptr};
  CacheEntry* next{nullptr};
};

using Evicted = std::vector<CacheEntry*>;

using BlockIndex = absl::node_hash_map<BlockHandle, CacheEntry, BlockHandleHash,
                                       std::equal_to<>>;

class EntryList {
 public:
  EntryList() { head_.next = head_.prev = &head_; }
  EntryList(const EntryList&) = delete;
  EntryList& operator=(const EntryList&) = delete;

  bool empty() const { return head_.next == &head_; }
  CacheEntry* Oldest() const { return empty() ? nullptr : head_.next; }
  CacheEntry* Next(const CacheEntry* e) const {
    return e->next == &head_ ? nullptr : e->next;
  }

  void PushBack(CacheEntry* e) {
    e->next = &head_;
    e->prev = head_.prev;
    head_.prev->next = e;
    head_.prev = e;
  }

  static void Remove(CacheEntry* e) {
    e->prev->next = e->next;
    e->next->prev = e->prev;
    e->prev = e->next = nullptr;
  }

  void MoveToNewest(CacheEntry* e) {
    Remove(e);
    PushBack(e);
  }

 private:
  CacheEntry head_;
};

class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;
  virtual void OnInsert(CacheEntry* entry) = 0;
  virtual void OnAccess(CacheEntry* entry) = 0;
  virtual void OnErase(CacheEntry* entry) = 0;
  virtual void Evict(uint64_t want_bytes, uint64_t want_files,
                     Evicted* to_del) = 0;
  virtual void EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                            uint64_t budget, Evicted* to_del) = 0;
};

using EvictionPolicyUPtr = std::unique_ptr<EvictionPolicy>;

class LruPolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override { list_.PushBack(entry); }
  void OnAccess(CacheEntry* entry) override { list_.MoveToNewest(entry); }
  void OnErase(CacheEntry* entry) override { EntryList::Remove(entry); }
  void Evict(uint64_t want_bytes, uint64_t want_files,
             Evicted* to_del) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    Evicted* to_del) override;

 private:
  EntryList list_;
};

class TwoRandomPolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override;
  void OnAccess(CacheEntry*) override {}
  void OnErase(CacheEntry* entry) override;
  void Evict(uint64_t want_bytes, uint64_t want_files,
             Evicted* to_del) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    Evicted* to_del) override;

 private:
  void RemoveEntry(CacheEntry* entry);
  std::vector<CacheEntry*> entries_;
};

class S3FifoPolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override;
  void OnAccess(CacheEntry* entry) override;
  void OnErase(CacheEntry* entry) override;
  void Evict(uint64_t want_bytes, uint64_t want_files,
             Evicted* to_del) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    Evicted* to_del) override;

 private:
  static constexpr uint8_t kFreqMask = 0x3;
  static constexpr uint8_t kMaxFreq = 3;
  static constexpr uint8_t kInMain = 0x4;
  static constexpr double kSmallRatio = 0.1;

  static uint8_t Freq(const CacheEntry* e) { return e->flags & kFreqMask; }
  static bool InMain(const CacheEntry* e) { return (e->flags & kInMain) != 0; }
  CacheEntry* EvictStep();
  CacheEntry* EvictSmallOne();
  CacheEntry* EvictMainOne();
  void RemoveFromQueue(CacheEntry* entry);
  bool GhostContains(const BlockHandle& handle) const;
  void GhostAdd(const BlockHandle& handle);
  void GhostRemove(const BlockHandle& handle);

  EntryList small_;
  EntryList main_;
  uint64_t small_bytes_{0};
  uint64_t main_bytes_{0};
  uint64_t small_count_{0};
  uint64_t main_count_{0};
  std::list<BlockHandle> ghost_fifo_;
  absl::flat_hash_map<BlockHandle, std::list<BlockHandle>::iterator,
                      BlockHandleHash, std::equal_to<>>
      ghost_pos_;
};

class SievePolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override;
  void OnAccess(CacheEntry* entry) override { entry->flags |= kVisited; }
  void OnErase(CacheEntry* entry) override;
  void Evict(uint64_t want_bytes, uint64_t want_files,
             Evicted* to_del) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    Evicted* to_del) override;

 private:
  static constexpr uint8_t kVisited = 0x1;
  void AdvanceHandPast(CacheEntry* entry);

  EntryList list_;
  CacheEntry* hand_{nullptr};
};

class NonePolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry*) override {}
  void OnAccess(CacheEntry*) override {}
  void OnErase(CacheEntry*) override {}
  void Evict(uint64_t, uint64_t, Evicted*) override {}
  void EvictExpired(uint32_t, uint32_t, uint64_t, Evicted*) override {}
};

EvictionPolicyUPtr NewEvictionPolicy(const std::string& name = "");

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_EVICTION_H_
