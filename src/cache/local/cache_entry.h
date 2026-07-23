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

#ifndef DINGOFS_SRC_CACHE_LOCAL_CACHE_ENTRY_H_
#define DINGOFS_SRC_CACHE_LOCAL_CACHE_ENTRY_H_

#include <absl/container/node_hash_map.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "common/block/block_handle.h"

namespace dingofs {
namespace cache {

// A cached block's in-memory record. There is exactly one CacheEntry per block,
// and it plays two roles at once: it is the value stored in the per-shard index
// AND the node linked into the eviction policy's ordering (a doubly-linked list
// or a sampling vector). Keeping the two roles in one object means a read hit
// costs a single index lookup and no allocation.
//
// The manager owns the entry's lifetime, `key/size/atime/staged`, and capacity
// accounting; the active EvictionPolicy owns only `prev/next/meta/aux`. Every
// field is mutated under the owning shard's mutex.
struct CacheEntry {
  // Points at the stable key stored inside the index node (see CacheIndex
  // below, which uses absl::node_hash_map for pointer stability). Used to
  // rebuild the on-disk path lazily and to erase the entry on eviction, so the
  // handle is never duplicated.
  const BlockHandle* key{nullptr};

  uint32_t size{0};             // block size in bytes
  std::atomic<uint32_t> atime;  // coarse last-access time in seconds (TTL)
  bool staged{false};           // writeback block pending upload: pinned, and
                                // never linked into the eviction policy

  // Eviction-policy-private fields. Which are used depends on the policy chosen
  // at construction (fixed for the shard's lifetime), so they never conflict.
  CacheEntry* prev{nullptr};    // intrusive list link (LRU/SIEVE/S3-FIFO)
  CacheEntry* next{nullptr};    // intrusive list link (LRU/SIEVE/S3-FIFO)
  std::atomic<uint8_t> meta;    // SIEVE: visited bit; S3-FIFO: 2-bit freq + tag
  uint32_t aux{0};              // 2-random: index into the sampling vector

  CacheEntry() : atime(0), meta(0) {}
};

using CacheVictims = std::vector<CacheEntry*>;

// Intrusive circular doubly-linked list over CacheEntry (via prev/next), shared
// by the list-based policies (LRU / SIEVE / S3-FIFO). The sentinel `head_` is
// not a real block: head_.next is the oldest entry, head_.prev the newest, so
// PushBack appends at the newest end and eviction walks from the oldest.
class CacheList {
 public:
  CacheList() { head_.next = head_.prev = &head_; }

  CacheList(const CacheList&) = delete;
  CacheList& operator=(const CacheList&) = delete;

  bool Empty() const { return head_.next == &head_; }
  CacheEntry* Sentinel() { return &head_; }
  CacheEntry* Oldest() const { return Empty() ? nullptr : head_.next; }
  CacheEntry* Newest() const { return Empty() ? nullptr : head_.prev; }

  void PushBack(CacheEntry* e) {  // insert at the newest end
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
  CacheEntry head_;  // sentinel, not a real block
};

struct BlockHandleHash {
  size_t operator()(const BlockHandle& handle) const { return handle.Hash(); }
};

// The per-shard cache index. node_hash_map keeps each value at a stable address
// so the eviction policy can hold raw CacheEntry* into it across insert/rehash.
using CacheIndex = absl::node_hash_map<BlockHandle, CacheEntry, BlockHandleHash,
                                       std::equal_to<>>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_CACHE_ENTRY_H_
