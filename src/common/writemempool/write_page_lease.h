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

#ifndef DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_PAGE_LEASE_H_
#define DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_PAGE_LEASE_H_

#include <cstddef>
#include <utility>

#include "absl/container/inlined_vector.h"

namespace dingofs {

class WriteMemPool;

// Move-only ownership of pages admitted by WriteMemPool. The lease returns
// every page not transferred with Take() when it is destroyed. A lease must
// not outlive its WriteMemPool.
class WritePageLease {
 public:
  WritePageLease() = default;
  ~WritePageLease();

  WritePageLease(const WritePageLease&) = delete;
  WritePageLease& operator=(const WritePageLease&) = delete;
  WritePageLease(WritePageLease&& other) noexcept;
  WritePageLease& operator=(WritePageLease&& other) noexcept;

  // Transfers exactly count pages to the caller. Acquire/TryAcquire callers
  // must size the lease for the write's worst-case page need.
  void Take(size_t count, char** pages);
  size_t Size() const { return pages_.size(); }
  bool Empty() const { return pages_.empty(); }

 private:
  friend class WriteMemPool;
  static constexpr size_t kInlinePages = 64;
  using Pages = absl::InlinedVector<char*, kInlinePages>;

  WritePageLease(WriteMemPool* pool, Pages pages)
      : pool_(pool), pages_(std::move(pages)) {}
  void Reset();

  WriteMemPool* pool_{nullptr};
  Pages pages_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_PAGE_LEASE_H_
