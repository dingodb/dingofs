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

#ifndef DINGOFS_SRC_CACHE_LOCAL_CACHE_POLICY_H_
#define DINGOFS_SRC_CACHE_LOCAL_CACHE_POLICY_H_

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "common/block/block_handle.h"

namespace dingofs {
namespace cache {

struct CacheEntry {
  const BlockHandle* key{nullptr};  // -> stable index key (lazy path, erase)
  uint32_t size{0};
  std::atomic<uint32_t> atime;  // coarse seconds, for TTL
  bool staged{false};

  CacheEntry* prev{nullptr};
  CacheEntry* next{nullptr};
  std::atomic<uint8_t>
      meta;         // sieve: visited bit; s3fifo: 2-bit freq + main bit
  uint32_t aux{0};  // 2random: index in the sampling vector

  CacheEntry() : atime(0), meta(0) {}
};

using CacheVictims = std::vector<CacheEntry*>;

struct BlockHandleHash {
  size_t operator()(const BlockHandle& handle) const { return handle.Hash(); }
};

// node_hash_map keeps values at stable addresses so the policy can hold raw
// CacheEntry* into it across insert/rehash.
using CacheIndex = absl::node_hash_map<BlockHandle, CacheEntry, BlockHandleHash,
                                       std::equal_to<>>;

class CacheList {
 public:
  CacheList() { head_.next = head_.prev = &head_; }
  CacheList(const CacheList&) = delete;
  CacheList& operator=(const CacheList&) = delete;

  bool Empty() const { return head_.next == &head_; }
  CacheEntry* Sentinel() { return &head_; }
  CacheEntry* Oldest() const { return Empty() ? nullptr : head_.next; }

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
                     CacheVictims* victims) = 0;
  virtual void EvictExpired(uint32_t now_sec, uint32_t expire_sec,
                            uint64_t budget, CacheVictims* victims) = 0;
};

using EvictionPolicyUPtr = std::unique_ptr<EvictionPolicy>;

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

class TwoRandomPolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* entry) override;
  void OnAccess(CacheEntry* /*entry*/) override {}
  void OnErase(CacheEntry* entry) override;
  void Evict(uint64_t want_bytes, uint64_t want_files,
             CacheVictims* victims) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    CacheVictims* victims) override;

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
             CacheVictims* victims) override;
  void EvictExpired(uint32_t now_sec, uint32_t expire_sec, uint64_t budget,
                    CacheVictims* victims) override;

 private:
  static constexpr uint8_t kFreqMask = 0x3;
  static constexpr uint8_t kMaxFreq = 3;
  static constexpr uint8_t kInMain = 0x4;
  static constexpr double kSmallRatio = 0.1;

  static uint8_t Freq(const CacheEntry* e);
  static bool InMain(const CacheEntry* e);
  CacheEntry* EvictStep();
  CacheEntry* EvictSmallOne();
  CacheEntry* EvictMainOne();
  void RemoveFromQueue(CacheEntry* entry);
  void ScanExpired(CacheList& list, uint32_t now_sec, uint32_t expire_sec,
                   uint64_t budget, uint64_t* checked, CacheVictims* victims);
  bool GhostContains(const BlockHandle& handle) const;
  void GhostAdd(const BlockHandle& handle);
  void GhostRemove(const BlockHandle& handle);

  CacheList small_;
  CacheList main_;
  uint64_t s_bytes_{0};
  uint64_t m_bytes_{0};
  uint64_t s_count_{0};
  uint64_t m_count_{0};
  std::list<BlockHandle> ghost_fifo_;
  absl::flat_hash_map<BlockHandle, std::list<BlockHandle>::iterator,
                      BlockHandleHash, std::equal_to<>>
      ghost_pos_;
};

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
  void AdvanceHandPast(CacheEntry* entry);

  CacheList list_;
  CacheEntry* hand_{nullptr};
};

class NonePolicy final : public EvictionPolicy {
 public:
  void OnInsert(CacheEntry* /*entry*/) override {}
  void OnAccess(CacheEntry* /*entry*/) override {}
  void OnErase(CacheEntry* /*entry*/) override {}
  void Evict(uint64_t, uint64_t, CacheVictims*) override {}
  void EvictExpired(uint32_t, uint32_t, uint64_t, CacheVictims*) override {}
};

EvictionPolicyUPtr NewEvictionPolicy(const std::string& name = "");

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_CACHE_POLICY_H_
