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

#include "common/writemempool/write_page_lease.h"

#include <glog/logging.h>

#include <utility>

#include "common/writemempool/write_mem_pool.h"

namespace dingofs {

WritePageLease::~WritePageLease() { Reset(); }

WritePageLease::WritePageLease(WritePageLease&& other) noexcept
    : pool_(std::exchange(other.pool_, nullptr)),
      pages_(std::move(other.pages_)) {
  other.pages_.clear();
}

WritePageLease& WritePageLease::operator=(WritePageLease&& other) noexcept {
  if (this == &other) return *this;
  Reset();
  pool_ = std::exchange(other.pool_, nullptr);
  pages_ = std::move(other.pages_);
  other.pages_.clear();
  return *this;
}

void WritePageLease::Take(size_t count, char** pages) {
  CHECK_LE(count, pages_.size());
  if (count == 0) return;
  CHECK_NOTNULL(pages);
  for (size_t i = 0; i < count; ++i) {
    pages[i] = pages_.back();
    pages_.pop_back();
  }
}

void WritePageLease::Reset() {
  if (pool_ != nullptr && !pages_.empty()) {
    pool_->Release(pages_.data(), pages_.size());
  }
  pages_.clear();
  pool_ = nullptr;
}

}  // namespace dingofs
