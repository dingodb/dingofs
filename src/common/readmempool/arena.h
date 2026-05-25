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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_ARENA_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_ARENA_H_

#include <cstddef>
#include <cstdint>
#include <memory>

namespace dingofs {

// 2MB. base is aligned to this so any >=4K sub-block stays 4K-aligned
// (O_DIRECT / future RDMA).
inline constexpr size_t kArenaAlign = 2ULL * 1024 * 1024;

// A pre-allocated, contiguous, non-growing memory region. The whole span is one
// contiguous virtual address range, so a single ibv_reg_mr can later cover every
// slot (see docs/designs/dingofs-read-mempool-impl.md section 10.1).
//
// Allocation path: (1) hugepage mmap(MAP_HUGETLB|MAP_POPULATE) -- on success it
//   is 2MB-aligned and pre-faulted; (2) on failure fall back to
//   posix_memalign(2MB), still 2MB-aligned (never bare malloc).
class Arena {
 public:
  // bytes is rounded up to align_to (see .cc). Returns nullptr on failure.
  static std::unique_ptr<Arena> Create(size_t bytes, size_t align_to);
  ~Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  uint8_t* Base() const { return base_; }    // always 2MB-aligned
  size_t Total() const { return total_; }     // = actual allocatable bytes
  bool HasHugePage() const { return huge_; }   // metrics only

 private:
  Arena(uint8_t* base, size_t total, bool huge)
      : base_(base), total_(total), huge_(huge) {}

  uint8_t* base_ = nullptr;
  size_t total_ = 0;
  bool huge_ = false;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_ARENA_H_
