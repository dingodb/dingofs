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

#include <sys/types.h>

#include <cstdint>

#include "bvar/passive_status.h"
#include "bvar/reducer.h"
#include "bvar/status.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {

class WriteMemPool {
 public:
  explicit WriteMemPool(int64_t total_bytes, int64_t page_size);

  ~WriteMemPool() = default;

  // Single try, never blocks -- the only allocation primitive. Safe under
  // write_flush_mutex_ / slice_mutex_ (SliceWriter, BlockData hold them). The
  // underlying MemoryPool::Require() already sweeps every shard and steals from
  // other caches, so a nullptr is a strong (near-)exhaustion signal, not a
  // casual shard-race miss. Any waiting/retry on a null is the caller's job
  // (FileWriter's lock-free throttle / short-write retry), never this layer.
  char* TryAllocate();

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

  // Count of TryAllocate() failures (pool returned null -> caller surfaces
  // ENOSPC / short write). Lock-free per-thread Adder, bumped only on the miss
  // branch.
  bvar::Adder<int64_t> alloc_fail_num_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
