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
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_S3FIFO_POLICY_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_S3FIFO_POLICY_H_

#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "cache/blockcache/eviction_policy.h"

namespace dingofs {
namespace cache {

// S3-FIFO (SOSP'23 "FIFO queues are all you need for cache eviction"):
//
//            ghost hit?
//   Add ──no──► small FIFO (~10%) ──evict(freq==0)──► ghost (fingerprints)
//         yes            └──freq>0: promote──┐
//          └───────────────► main FIFO (~90%)┘ (evict: freq>0 reinserts)
//
// New blocks start in the small probationary queue; one-hit wonders leave it
// quickly and only their fingerprint is remembered in the ghost queue — a
// re-fetch within the ghost window is readmitted straight into main. A hit
// only bumps a saturating 2-bit counter (no reordering), promotion happens
// lazily at eviction time. Scan traffic churns the small queue and never
// touches the main 90%.
class S3FIFOPolicy final : public EvictionPolicy {
 public:
  S3FIFOPolicy(uint64_t capacity_bytes, double small_ratio);
  ~S3FIFOPolicy() override = default;

  void Add(const CacheKey& key, const CacheValue& value) override;
  bool Touch(const CacheKey& key, CacheValue* value) override;
  bool Exist(const CacheKey& key) const override;
  bool Get(const CacheKey& key, CacheValue* value) const override;
  bool Delete(const CacheKey& key, CacheValue* deleted) override;
  CacheItems Evict(FilterFunc filter) override;
  CacheItems Sweep(FilterFunc filter) override;

  size_t Size() const override;
  void Clear() override;

  EvictionPolicyStats GetStats() const override {
    return {small_bytes_, ghost_fifo_.size(), ghost_hits_};
  }

 private:
  struct Node {
    CacheValue value;
    Node* prev{nullptr};
    Node* next{nullptr};
    const std::string* key{nullptr};  // -> map key (node-based, stable)
    bool in_small{false};
  };

  using Map = std::unordered_map<std::string, Node>;

  // circular sentinel queues: newest at prev side, oldest = sentinel->next
  static void QueueInit(Node* queue);
  static void QueuePush(Node* queue, Node* node);
  static void QueueRemove(Node* node);
  static bool QueueEmpty(const Node* queue);

  static uint64_t Fingerprint(const std::string& filename);

  Node* NextVictim();
  void MoveToMain(Node* node);
  CacheItem RemoveNode(Node* node, bool remember_in_ghost);
  void GhostInsert(uint64_t fingerprint);
  bool GhostErase(uint64_t fingerprint);

  Map nodes_;
  Node small_;
  Node main_;
  uint64_t small_bytes_{0};
  const uint64_t small_capacity_;
  uint64_t ghost_hits_{0};

  // fingerprints of blocks evicted from small, FIFO-bounded by the number of
  // resident entries (min 4096)
  std::unordered_set<uint64_t> ghost_;
  std::deque<uint64_t> ghost_fifo_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_S3FIFO_POLICY_H_
