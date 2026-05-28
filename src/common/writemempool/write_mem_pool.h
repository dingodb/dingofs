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

#include <atomic>
#include <chrono>
#include <cstdint>

#include "bvar/passive_status.h"
#include "bvar/status.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {

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

 private:
  static int64_t UsedBytes(void* arg) {
    auto* manager = reinterpret_cast<WriteMemPool*>(arg);
    return manager->GetUsedBytes();
  }

  static int64_t UsedPages(void* arg) {
    auto* manager = reinterpret_cast<WriteMemPool*>(arg);
    return manager->used_pages_.load();
  }

  const int64_t total_bytes_{0};
  const int64_t page_size_{0};
  std::atomic<int64_t> used_pages_{0};
  MemoryPoolUPtr pool_;

  bvar::Status<int64_t> write_buffer_total_bytes_;
  bvar::PassiveStatus<int64_t> write_buffer_used_pages_;
  bvar::PassiveStatus<int64_t> write_buffer_used_bytes_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
