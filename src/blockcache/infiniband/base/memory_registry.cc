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

#include "blockcache/infiniband/base/memory_registry.h"

#include <glog/logging.h>

#include <cerrno>
#include <utility>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

MemoryRegistry::MemoryRegistry(ibv_pd* pd) : pd_(pd) {
  CHECK(tls_memory_registry == nullptr) << "one memory registry per shard";
  tls_memory_registry = this;
}

MemoryRegistry::~MemoryRegistry() { tls_memory_registry = nullptr; }

StatusOr<const MemoryRegion*> MemoryRegistry::Register(void* addr,
                                                       size_t length,
                                                       unsigned access) {
  StatusOr<MemoryRegion> mr = MemoryRegion::Register(pd_, addr, length, access);
  if (!mr.ok()) {
    return mr.status();
  }
  regions_.push_back(std::move(mr).value());
  return &regions_.back();
}

StatusOr<uint32_t> MemoryRegistry::GetLKey(const void* p, size_t len) const {
  StatusOr<const MemoryRegion*> mr = Find(p, len);
  if (!mr.ok()) {
    return mr.status();
  }
  return mr.value()->lkey();
}

StatusOr<uint32_t> MemoryRegistry::GetRKey(const void* p, size_t len) const {
  StatusOr<const MemoryRegion*> mr = Find(p, len);
  if (!mr.ok()) {
    return mr.status();
  }
  return mr.value()->rkey();
}

StatusOr<const MemoryRegion*> MemoryRegistry::Find(const void* p,
                                                   size_t len) const {
  const auto* c = static_cast<const char*>(p);
  for (const MemoryRegion& mr : regions_) {
    const auto* mr_base = static_cast<const char*>(mr.addr());
    if (c >= mr_base && c + len <= mr_base + mr.length()) {
      return &mr;
    }
  }
  return ToStatus(EFAULT, "find a registration covering the buffer");
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
