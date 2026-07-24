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
#include "common/writemempool/write_page_pool.h"

namespace dingofs {

class WriteMemPool {
 public:
  explicit WriteMemPool(int64_t total_bytes, int64_t page_size);

  ~WriteMemPool() = default;

  // Try-style capacity semantics: never waits for pages to be returned and
  // does not use a capacity wait queue. Internal synchronization may be
  // briefly contended. Each call only consumes pages currently available in
  // the backing pool, then returns the pages acquired so far; the caller owns
  // rollback and any later retry.
  size_t TryAllocateBatch(size_t count, char** pages);

  // Returns exactly `count` pairwise-distinct outstanding pages previously
  // obtained from this pool. Duplicate, stale, or foreign pages violate the
  // contract and may corrupt the underlying intrusive free lists.
  void DeAllocateBatch(char* const* pages, size_t count);

  int64_t GetPageSize() const;

  int64_t GetTotalBytes() const;

  int64_t GetUsedBytes() const;

  double GetUsageRatio() const;

  bool IsHighPressure(double threshold = 0.8) const;

  // RDMA-ready: expose the complete fixed-page arena geometry for MR
  // registration, buffer descriptor construction, and address-to-index
  // conversion.
  char* BaseAddr() const;
  size_t BufferSize() const;
  size_t BufferCount() const;
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

  // Outstanding pages. bvar::Adder keeps updates sharded per thread; reads
  // aggregate only on throttle checks and bvar sampling.
  bvar::Adder<int64_t> used_pages_;

  WritePagePoolUPtr page_pool_;

  // Capacity / usage. PassiveStatus is sampled on /vars dump (off the hot
  // path); updates use the sharded reducer above.
  bvar::Status<int64_t> capacity_pages_;
  bvar::PassiveStatus<int64_t> used_pages_var_;
  bvar::PassiveStatus<int64_t> used_bytes_var_;

  // Count of allocation requests that were not fully satisfied.
  bvar::Adder<int64_t> alloc_fail_num_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
