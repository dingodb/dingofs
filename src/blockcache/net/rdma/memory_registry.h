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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_MEMORY_REGISTRY_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_MEMORY_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <deque>

#include "blockcache/common/status.h"
#include "blockcache/net/infiniband/buffer.h"
#include "blockcache/net/infiniband/device.h"
#include "blockcache/net/infiniband/memory_region.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

// Per-shard registration table. The whole DMA pool is one memory region, so
// the hot path never consults this; external registrations are for buffers
// outside the pool and a hot caller should cache what Find returns.
class MemoryRegistry {
 public:
  MemoryRegistry() = default;

  // Registers [base, base + length) as the pool region.
  Status Init(ibv_pd* pd, void* base, size_t length);

  // The returned pointer stays valid for the table's lifetime.
  StatusOr<const verbs::MemoryRegion*> Register(
      void* addr, size_t length,
      unsigned access = verbs::MemoryRegion::kDefaultAccess);

  void Adopt(verbs::MemoryRegion mr) { external_.push_back(std::move(mr)); }

  // O(1) inside the pool, linear over the (few) external regions.
  StatusOr<const verbs::MemoryRegion*> Find(const void* p, size_t len) const;
  StatusOr<uint32_t> LkeyOf(const void* p, size_t len) const;
  StatusOr<uint32_t> RkeyOf(const void* p, size_t len) const;

  bool InPool(const void* p, size_t len) const {
    const auto* c = static_cast<const char*>(p);
    return c >= base_ && c + len <= base_ + length_;
  }
  uint32_t pool_lkey() const { return pool_mr_.lkey(); }
  uint32_t pool_rkey() const { return pool_mr_.rkey(); }
  char* base() const { return base_; }
  size_t length() const { return length_; }

 private:
  verbs::MemoryRegion pool_mr_;
  ibv_pd* pd_ = nullptr;
  char* base_ = nullptr;
  size_t length_ = 0;
  std::deque<verbs::MemoryRegion> external_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_MEMORY_REGISTRY_H_
