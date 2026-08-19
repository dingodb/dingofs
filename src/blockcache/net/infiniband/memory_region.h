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

#ifndef DINGOFS_BLOCKCACHE_NET_INFINIBAND_MEMORY_REGION_H_
#define DINGOFS_BLOCKCACHE_NET_INFINIBAND_MEMORY_REGION_H_

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace verbs {

// Move-only RAII over one ibv_mr; register few, large regions.
class MemoryRegion {
 public:
  static constexpr unsigned kDefaultAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

  MemoryRegion() = default;
  ~MemoryRegion() { Reset(); }

  MemoryRegion(MemoryRegion&& o) noexcept : mr_(o.mr_) { o.mr_ = nullptr; }
  MemoryRegion& operator=(MemoryRegion&& o) noexcept {
    if (this != &o) {
      Reset();
      mr_ = o.mr_;
      o.mr_ = nullptr;
    }
    return *this;
  }
  MemoryRegion(const MemoryRegion&) = delete;
  MemoryRegion& operator=(const MemoryRegion&) = delete;

  static StatusOr<MemoryRegion> Register(ibv_pd* pd, void* addr, size_t length,
                                         unsigned access = kDefaultAccess);

  bool Valid() const { return mr_ != nullptr; }
  ibv_mr* get() const { return mr_; }
  uint32_t lkey() const { return mr_->lkey; }
  uint32_t rkey() const { return mr_->rkey; }
  void* addr() const { return mr_->addr; }
  size_t length() const { return mr_->length; }

 private:
  explicit MemoryRegion(ibv_mr* mr) : mr_(mr) {}
  void Reset() noexcept;

  ibv_mr* mr_ = nullptr;
};

}  // namespace verbs
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_INFINIBAND_MEMORY_REGION_H_
