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

#include "common/writemempool/write_mem_pool.h"

#include <glog/logging.h>

namespace dingofs {

WriteMemPool::WriteMemPool(int64_t total_bytes, int64_t page_size)
    : total_bytes_(total_bytes),
      page_size_(page_size),
      pool_(MemoryPool::Create(static_cast<size_t>(page_size),
                               static_cast<size_t>(total_bytes / page_size))),
      capacity_pages_("vfs_write_pool_capacity_pages",
                      page_size > 0 ? total_bytes / page_size : 0),
      used_pages_var_("vfs_write_pool_used_pages", UsedPages, this),
      used_bytes_var_("vfs_write_pool_used_bytes", UsedBytes, this),
      alloc_fail_num_("vfs_write_pool_alloc_fail_num") {
  CHECK(pool_ != nullptr) << "WriteMemPool failed to create MemoryPool: page="
                          << page_size
                          << " count=" << (total_bytes / page_size);
}

char* WriteMemPool::TryAllocate() {
  // One Require(): try-semantics, never blocks, so it is safe under
  // write_flush_mutex_ / slice_mutex_. Require() already sweeps all shards and
  // steals from other caches internally, so a nullptr is a strong (near-)
  // exhaustion signal; the caller (FileWriter throttle / short-write retry)
  // owns any waiting, never this lock-holding layer.
  char* page = pool_->Require();
  if (page != nullptr) {
    used_pages_ << 1;
  } else {
    alloc_fail_num_ << 1;
  }
  return page;
}

void WriteMemPool::DeAllocate(char* page) {
  if (page == nullptr) {
    return;
  }
  pool_->Release(page);
  used_pages_ << -1;
}

int64_t WriteMemPool::GetPageSize() const { return page_size_; }

int64_t WriteMemPool::GetTotalBytes() const { return total_bytes_; }

int64_t WriteMemPool::GetUsedBytes() const {
  return page_size_ * used_pages_.get_value();
}

double WriteMemPool::GetUsageRatio() const {
  int64_t total = GetTotalBytes();
  if (total == 0) {
    return 0.0;
  }
  return static_cast<double>(GetUsedBytes()) / total;
}

bool WriteMemPool::IsHighPressure(double threshold) const {
  return GetUsageRatio() >= threshold;
}

char* WriteMemPool::BaseAddr() const { return pool_->BaseAddr(); }

size_t WriteMemPool::TotalSize() const { return pool_->TotalSize(); }

}  // namespace dingofs