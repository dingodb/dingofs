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

#include "client/vfs/memory/write_buffer_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include "common/helper.h"

namespace dingofs {
namespace client {
namespace vfs {

WriteBufferManager::WriteBufferManager(int64_t total_bytes, int64_t page_size)
    : total_bytes_(total_bytes),
      page_size_(page_size),
      write_buffer_total_bytes_("vfs_write_buffer_total_bytes_", total_bytes),
      write_buffer_used_pages_("vfs_write_buffer_used_pages", UsedPages, this),
      write_buffer_used_bytes_("vfs_write_buffer_used_bytes", UsedBytes, this) {
}

char* WriteBufferManager::Allocate() {
  butil::Timer timer;
  timer.start();
  char* page = new char[page_size_];
  timer.stop();

  VLOG(16) << fmt::format("Allocate page at: {} took: <{:.6f}> ms",
                          Helper::Char2Addr(page), timer.u_elapsed(0.0));

  used_pages_.fetch_add(1);
  return page;
}

void WriteBufferManager::DeAllocate(char* page) {
  butil::Timer timer;
  timer.start();
  delete[] page;
  timer.stop();

  VLOG(16) << fmt::format(
      "Deallocating page at: {} allocation took: <{:.6f}> ms",
      Helper::Char2Addr(page), timer.u_elapsed(0.0));

  used_pages_.fetch_sub(1);
}

int64_t WriteBufferManager::GetPageSize() const { return page_size_; }

int64_t WriteBufferManager::GetTotalBytes() const { return total_bytes_; }

int64_t WriteBufferManager::GetUsedBytes() const {
  return page_size_ * used_pages_.load(std::memory_order_relaxed);
}

double WriteBufferManager::GetUsageRatio() const {
  int64_t total = GetTotalBytes();
  if (total == 0) {
    return 0.0;
  }
  return static_cast<double>(GetUsedBytes()) / total;
}

bool WriteBufferManager::IsHighPressure(double threshold) const {
  return GetUsageRatio() >= threshold;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs