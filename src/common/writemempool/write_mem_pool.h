/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
#define DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_

#include <gflags/gflags.h>
#include <sys/types.h>

#include <cstdint>

#include "bvar/latency_recorder.h"
#include "bvar/passive_status.h"
#include "bvar/reducer.h"
#include "bvar/status.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {

// Bounded-acquire deadline (ms) for one write-page Allocate(). Defined in
// write_mem_pool.cc; both DEFINE and DECLARE live in namespace dingofs (dingofs
// keeps its gflags inside the namespace, see options/client.cc).
DECLARE_int64(vfs_write_pool_acquire_timeout_ms);

class WriteMemPool {
 public:
  explicit WriteMemPool(int64_t total_bytes, int64_t page_size);

  ~WriteMemPool() = default;

  char* Allocate();

  void DeAllocate(char* page);

  int64_t GetPageSize() const;

  int64_t GetTotalBytes() const;

  int64_t GetUsedBytes() const;

  double GetUsageRatio() const;

  bool IsHighPressure(double threshold = 0.8) const;

  // RDMA-ready: forward the underlying pool's contiguous arena base + total
  // length for ibv_reg_mr. Mirrors ReadMemPool::BaseAddr()/TotalSize().
  char* BaseAddr() const;
  size_t TotalSize() const;

 private:
  static int64_t UsedBytes(void* arg) {
    auto* manager = reinterpret_cast<WriteMemPool*>(arg);
    return manager->GetUsedBytes();
  }

  static int64_t UsedPages(void* arg) {
    auto* manager = reinterpret_cast<WriteMemPool*>(arg);
    return manager->used_pages_.get_value();
  }

  const int64_t total_bytes_{0};
  const int64_t page_size_{0};
  // Outstanding page count. A single shared atomic here was a true-sharing
  // bottleneck: every Allocate/DeAllocate RMW'd one cache line, so the
  // per-thread-cache MemoryPool below it could not scale (measured 64-thread
  // alloc went 15ns->2939ns). bvar::Adder is per-thread sharded -- "<< 1" hits
  // only the caller's shard; get_value() aggregates and is read only on the
  // throttle check / bvar dump, not per page.
  bvar::Adder<int64_t> used_pages_;
  MemoryPoolUPtr pool_;

  // Capacity / usage. PassiveStatus is sampled on /vars dump (off the hot
  // path); the underlying used_pages_ atomic is the only per-op cost and it
  // predates pooling.
  bvar::Status<int64_t> capacity_pages_;
  bvar::PassiveStatus<int64_t> used_pages_var_;
  bvar::PassiveStatus<int64_t> used_bytes_var_;

  // Slow-path counters (lock-free per-thread Adder). Bumped ONLY inside
  // Allocate's bounded-acquire failure/retry branch -- the fast path (first
  // Require() hit, ~99%) never touches these, so steady-state alloc cost is
  // unchanged. The retry branch already yields/sleeps (us~ms), so the Adder's
  // few ns are noise there.
  bvar::Adder<int64_t> acquire_fail_num_;    // bounded-acquire timed out
  bvar::Adder<int64_t> transient_null_num_;  // a TryRequire() returned null
  bvar::Adder<int64_t> acquire_wait_num_;    // requests that entered retry
  bvar::LatencyRecorder acquire_wait_us_;    // time spent in bounded-acquire
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
