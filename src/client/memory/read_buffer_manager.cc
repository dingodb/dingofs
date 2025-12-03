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

#include "client/memory/read_buffer_manager.h"

#include <glog/logging.h>

namespace dingofs {
namespace client {

ReadBufferManager::ReadBufferManager(int64_t total_bytes)
    : total_bytes_(total_bytes),
      used_bytes_(0),
      read_buffer_total_bytes_("dingofs_read_buffer_total_bytes", total_bytes),
      read_buffer_used_bytes_("dingofs_read_buffer_used_bytes") {}

void ReadBufferManager::Take(int64_t bytes) {
  VLOG(12) << "ReadBufferManager::Take bytes: " << bytes;

  CHECK(bytes > 0) << "bytes must be positive";
  used_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  read_buffer_used_bytes_ << bytes;
}

void ReadBufferManager::Release(int64_t bytes) {
  VLOG(12) << "ReadBufferManager::Release bytes: " << bytes;

  CHECK(bytes > 0) << "bytes must be positive";
  used_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
  read_buffer_used_bytes_ << -bytes;
}

int64_t ReadBufferManager::GetTotalBytes() const { return total_bytes_; }

int64_t ReadBufferManager::GetUsedBytes() const {
  return used_bytes_.load(std::memory_order_relaxed);
}

double ReadBufferManager::GetUsageRatio() const {
  int64_t total = GetTotalBytes();
  if (total == 0) {
    return 0.0;
  }
  return static_cast<double>(GetUsedBytes()) / total;
}

bool ReadBufferManager::IsHighPressure(double threshold) const {
  return GetUsageRatio() >= threshold;
}

}  // namespace client
}  // namespace dingofs