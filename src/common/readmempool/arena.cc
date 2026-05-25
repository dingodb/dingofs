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

#include "common/readmempool/arena.h"

#include <glog/logging.h>
#include <sys/mman.h>

#include <cstdlib>

namespace dingofs {

namespace {
size_t AlignUp(size_t v, size_t a) { return (v + a - 1) & ~(a - 1); }
}  // namespace

std::unique_ptr<Arena> Arena::Create(size_t bytes, size_t align_to) {
  CHECK_GT(bytes, 0u);
  // Round up to align_to (= max-order block size, so we get whole top blocks).
  size_t total = AlignUp(bytes, align_to);

  // (1) hugepage path: MAP_HUGETLB needs the length to be a 2MB multiple (total
  //     already is); the returned address is hugepage(2MB)-aligned, and
  //     MAP_POPULATE pre-faults it.
  bool huge = true;
  void* p = ::mmap(nullptr, total, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE, -1,
                   0);
  if (p == MAP_FAILED) {
    // (2) fallback: posix_memalign(2MB), still 2MB-aligned (not bare malloc,
    //     which would only give 16B alignment and break the 4K-alignment premise).
    huge = false;
    p = nullptr;
    if (::posix_memalign(&p, kArenaAlign, total) != 0 || p == nullptr) {
      LOG(ERROR) << "Arena::Create failed: bytes=" << bytes
                 << " total=" << total;
      return nullptr;
    }
  }

  auto* base = static_cast<uint8_t*>(p);
  CHECK_EQ(reinterpret_cast<uintptr_t>(base) % kArenaAlign, 0u)
      << "arena base must be " << kArenaAlign << "-aligned";

  LOG(INFO) << "Arena created: total=" << total << " huge=" << huge
            << " base=" << static_cast<void*>(base);
  // Private ctor, wrapped with new.
  return std::unique_ptr<Arena>(new Arena(base, total, huge));
}

Arena::~Arena() {
  if (base_ == nullptr) return;
  if (huge_) {
    ::munmap(base_, total_);
  } else {
    ::free(base_);
  }
  base_ = nullptr;
}

}  // namespace dingofs
