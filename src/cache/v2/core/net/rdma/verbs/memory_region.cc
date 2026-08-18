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

#include "cache/v2/core/net/rdma/verbs/memory_region.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstring>

#include "cache/v2/common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

void MemoryRegion::Reset() noexcept {
  if (mr_ != nullptr) {
    // Not PLOG: ibv_dereg_mr returns the errno value, it does not set errno.
    if (const int rc = ibv_dereg_mr(mr_); rc != 0) {
      LOG(ERROR) << "Fail to deregister memory region: " << std::strerror(rc);
    }
    mr_ = nullptr;
  }
}

StatusOr<MemoryRegion> MemoryRegion::Register(ibv_pd* pd, void* addr,
                                              size_t length, unsigned access) {
  ibv_mr* mr = ibv_reg_mr(pd, addr, length, static_cast<int>(access));
  if (mr == nullptr) {
    return ToStatus(errno, "register memory region");
  }
  return MemoryRegion(mr);
}

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs
