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

#include <limits>

namespace dingofs {
namespace {

WritePagePoolUPtr CreatePagePool(int64_t total_bytes, int64_t page_size) {
  CHECK_GT(page_size, 0);
  CHECK_GE(total_bytes, page_size);
  CHECK_EQ(total_bytes % page_size, 0)
      << "write pool capacity must be an exact multiple of page size";
  CHECK_LE(static_cast<uint64_t>(total_bytes),
           static_cast<uint64_t>(std::numeric_limits<size_t>::max()));

  auto pool =
      WritePagePool::Create(static_cast<size_t>(page_size),
                            static_cast<size_t>(total_bytes / page_size));
  CHECK(pool != nullptr) << "WriteMemPool failed to create WritePagePool: page="
                         << page_size << " total_bytes=" << total_bytes;
  return pool;
}

}  // namespace

WriteMemPool::WriteMemPool(int64_t total_bytes, int64_t page_size)
    : total_bytes_(total_bytes),
      page_size_(page_size),
      page_pool_(CreatePagePool(total_bytes, page_size)),
      capacity_pages_("vfs_write_pool_capacity_pages",
                      static_cast<int64_t>(page_pool_->BufferCount())),
      used_pages_var_("vfs_write_pool_used_pages", UsedPages, this),
      used_bytes_var_("vfs_write_pool_used_bytes", UsedBytes, this),
      alloc_fail_num_("vfs_write_pool_alloc_fail_num") {}

size_t WriteMemPool::TryAllocateBatch(size_t count, char** pages) {
  const size_t allocated = page_pool_->RequireBatch(pages, count);
  if (allocated != 0) {
    used_pages_ << static_cast<int64_t>(allocated);
  }
  if (allocated != count) {
    alloc_fail_num_ << 1;
  }
  return allocated;
}

void WriteMemPool::DeAllocateBatch(char* const* pages, size_t count) {
  if (count == 0) return;
  page_pool_->ReleaseBatch(pages, count);
  used_pages_ << -static_cast<int64_t>(count);
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

char* WriteMemPool::BaseAddr() const { return page_pool_->BaseAddr(); }

size_t WriteMemPool::BufferSize() const { return page_pool_->BufferSize(); }

size_t WriteMemPool::BufferCount() const { return page_pool_->BufferCount(); }

size_t WriteMemPool::TotalSize() const { return page_pool_->TotalSize(); }

}  // namespace dingofs
