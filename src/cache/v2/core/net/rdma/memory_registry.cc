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

#include "cache/v2/core/net/rdma/memory_registry.h"

#include <cerrno>
#include <utility>

#include "cache/v2/common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

Status MemoryRegistry::Init(ibv_pd* pd, void* base, size_t length) {
  StatusOr<verbs::MemoryRegion> mr =
      verbs::MemoryRegion::Register(pd, base, length);
  if (!mr.ok()) {
    return mr.status();
  }
  pool_mr_ = std::move(mr).value();
  pd_ = pd;
  base_ = static_cast<char*>(base);
  length_ = length;
  return Status::OK();
}

StatusOr<const verbs::MemoryRegion*> MemoryRegistry::Register(void* addr,
                                                              size_t length,
                                                              unsigned access) {
  StatusOr<verbs::MemoryRegion> mr =
      verbs::MemoryRegion::Register(pd_, addr, length, access);
  if (!mr.ok()) {
    return mr.status();
  }
  external_.push_back(std::move(mr).value());
  return &external_.back();
}

StatusOr<const verbs::MemoryRegion*> MemoryRegistry::Find(const void* p,
                                                          size_t len) const {
  if (InPool(p, len)) {
    return &pool_mr_;
  }
  const auto* c = static_cast<const char*>(p);
  for (const verbs::MemoryRegion& mr : external_) {
    const auto* mr_base = static_cast<const char*>(mr.addr());
    if (c >= mr_base && c + len <= mr_base + mr.length()) {
      return &mr;
    }
  }
  return ToStatus(EFAULT, "find a registration covering the buffer");
}

StatusOr<uint32_t> MemoryRegistry::LkeyOf(const void* p, size_t len) const {
  StatusOr<const verbs::MemoryRegion*> mr = Find(p, len);
  if (!mr.ok()) {
    return mr.status();
  }
  return mr.value()->lkey();
}

StatusOr<uint32_t> MemoryRegistry::RkeyOf(const void* p, size_t len) const {
  StatusOr<const verbs::MemoryRegion*> mr = Find(p, len);
  if (!mr.ok()) {
    return mr.status();
  }
  return mr.value()->rkey();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
