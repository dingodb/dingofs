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

/*
 * Project: DingoFS
 * Created Date: 2026-06-02
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/rdma_region_registry.h"

#include <glog/logging.h>

#include <mutex>
#include <utility>

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

RDMARegionRegistry& RDMARegionRegistry::GetInstance() {
  static RDMARegionRegistry instance;
  return instance;
}

uint32_t RDMARegionRegistry::Register(void* base, size_t length) {
  if (!FLAGS_use_rdma || base == nullptr || length == 0) {
    return 0;
  }

  // Register against the SAME per-device singleton protection domain the cache
  // client's RDMA QPs and buffer pool use (Infiniband::GetOrAlloc caches the PD
  // by device name), so the region's rkey is valid on those QPs.
  infiniband::Infiniband::Context ctx;
  auto status = infiniband::Infiniband::Init(
      FLAGS_cache_rdma_device, static_cast<uint8_t>(FLAGS_cache_rdma_port_num),
      &ctx);
  if (!status.ok()) {
    LOG(ERROR) << "RDMARegionRegistry: infiniband init failed: "
               << status.ToString();
    return 0;
  }

  auto mr = infiniband::MemoryRegion::Register(ctx.protect_domain, base, length);
  if (mr == nullptr) {
    LOG(ERROR) << "RDMARegionRegistry: ibv_reg_mr failed for base=" << base
               << " length=" << length;
    return 0;
  }
  uint32_t rkey = mr->GetRkey();

  std::lock_guard<bthread::Mutex> lock(mutex_);
  regions_.push_back(Region{reinterpret_cast<uintptr_t>(base), length, rkey});
  owned_.push_back(std::move(mr));
  LOG(INFO) << "RDMARegionRegistry: registered base=" << base
            << " length=" << length << " rkey=" << rkey;
  return rkey;
}

uint32_t RDMARegionRegistry::RkeyFor(const void* p, size_t n) {
  if (p == nullptr || n == 0) {
    return 0;
  }
  const auto start = reinterpret_cast<uintptr_t>(p);
  const auto end = start + n;
  if (end < start) {
    return 0;
  }

  std::lock_guard<bthread::Mutex> lock(mutex_);
  for (const auto& region : regions_) {
    const auto region_end = region.addr + region.length;
    if (region_end < region.addr) {
      continue;
    }
    if (start >= region.addr && end <= region_end) {
      return region.rkey;
    }
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
