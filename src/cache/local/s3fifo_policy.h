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

#ifndef DINGOFS_SRC_CACHE_LOCAL_S3FIFO_POLICY_H_
#define DINGOFS_SRC_CACHE_LOCAL_S3FIFO_POLICY_H_

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <functional>
#include <list>

#include "cache/local/cache_entry.h"
#include "cache/local/cache_policy.h"
#include "common/block/block_handle.h"

namespace dingofs {
namespace cache {

// S3-FIFO (SOSP'23, "FIFO queues are all you need"): a small FIFO S (~10% of
// bytes), a main FIFO M, and a ghost queue of recently-evicted keys, plus a
// 2-bit frequency counter per entry. New entries enter S (or M, on a ghost
// hit). A hit only bumps the counter -- no movement. On eviction S filters
// one-hit-wonders (a scan) out before they can reach M, which is what makes it
// scan-resistant while staying O(1) and lock-friendly.
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
  // meta layout: bits [0..1] = frequency (0..3), bit [2] = in main queue.
  static constexpr uint8_t kFreqMask = 0x3;
  static constexpr uint8_t kMaxFreq = 3;
  static constexpr uint8_t kInMain = 0x4;
  static constexpr double kSmallRatio = 0.1;

  static uint8_t Freq(const CacheEntry* e);
  static bool InMain(const CacheEntry* e);

  // One eviction step; returns the evicted entry, or nullptr if the step only
  // promoted/reinserted (which frees no bytes).
  CacheEntry* EvictStep();
  CacheEntry* EvictSmallOne();
  CacheEntry* EvictMainOne();

  void RemoveFromQueue(CacheEntry* entry);  // unlink + fix byte/count counters
  void ScanExpired(CacheList& list, bool in_main, uint32_t now_sec,
                   uint32_t expire_sec, uint64_t budget, uint64_t* checked,
                   CacheVictims* victims);

  bool GhostContains(const BlockHandle& handle) const;
  void GhostAdd(const BlockHandle& handle);
  void GhostRemove(const BlockHandle& handle);

  CacheList small_;
  CacheList main_;
  uint64_t s_bytes_{0};
  uint64_t m_bytes_{0};
  uint64_t s_count_{0};
  uint64_t m_count_{0};

  std::list<BlockHandle> ghost_fifo_;  // front = oldest ghost key
  absl::flat_hash_map<BlockHandle, std::list<BlockHandle>::iterator,
                      BlockHandleHash, std::equal_to<>>
      ghost_pos_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_LOCAL_S3FIFO_POLICY_H_
