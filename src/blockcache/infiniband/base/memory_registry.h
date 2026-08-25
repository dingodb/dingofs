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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_BASE_MEMORY_REGISTRY_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_BASE_MEMORY_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/memory_region.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class MemoryRegistry {
 public:
  explicit MemoryRegistry(ibv_pd* pd);
  ~MemoryRegistry();

  MemoryRegistry(const MemoryRegistry&) = delete;
  MemoryRegistry& operator=(const MemoryRegistry&) = delete;

  StatusOr<const MemoryRegion*> Register(
      void* addr, size_t length,
      unsigned access = MemoryRegion::kDefaultAccess);

  const MemoryRegion* Add(MemoryRegion mr) {
    regions_.push_back(std::move(mr));
    return &regions_.back();
  }

  StatusOr<uint32_t> GetLKey(const void* p, size_t len) const;
  StatusOr<uint32_t> GetRKey(const void* p, size_t len) const;

 private:
  StatusOr<const MemoryRegion*> Find(const void* p, size_t len) const;

  ibv_pd* pd_ = nullptr;
  std::deque<MemoryRegion> regions_;
};

using MemoryRegistryUPtr = std::unique_ptr<MemoryRegistry>;

inline thread_local MemoryRegistry* tls_memory_registry = nullptr;
inline MemoryRegistry* ThisMemoryRegistry() { return tls_memory_registry; }

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_BASE_MEMORY_REGISTRY_H_
