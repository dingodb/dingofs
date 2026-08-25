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

#include "blockcache/infiniband/base/memory_region.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstring>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

StatusOr<MemoryRegion> MemoryRegion::Register(ibv_pd* pd, void* addr,
                                              size_t length, unsigned access) {
  ibv_mr* mr = ibv_reg_mr(pd, addr, length, static_cast<int>(access));
  if (mr == nullptr) {
    return ToStatus(errno, "register memory region");
  }

  LOG(INFO) << "Successfully register MemoryRegion{device="
            << ibv_get_device_name(pd->context->device) << " addr=" << addr
            << " length=" << length << " lkey=" << mr->lkey
            << " rkey=" << mr->rkey << "}";
  return MemoryRegion(mr);
}

void MemoryRegion::Reset() noexcept {
  if (mr_ == nullptr) {
    return;
  }

  const int rc = ibv_dereg_mr(mr_);
  if (rc != 0) {
    LOG(ERROR) << "Fail to deregister memory region: " << std::strerror(rc);
  }
  mr_ = nullptr;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
