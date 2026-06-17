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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_REGION_REGISTRY_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_REGION_REGISTRY_H_

#include <bthread/mutex.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "cache/infiniband/infiniband.h"

namespace dingofs {
namespace cache {

// Process-wide registry of RDMA-registered client memory regions (e.g. the FUSE
// read / write mempools). It lets the cache layer resolve the rkey of a
// caller-provided buffer by address, so a registered Range destination is
// RDMA-written straight in and a registered Put source is RDMA-read straight out
// -- zero copy. Regions are registered against the cache RDMA device's shared
// protection domain (Infiniband::GetOrAlloc, the same PD the client's RDMA QPs
// and buffer pool use), so the rkey is valid for the server's one-sided RDMA.
class RDMARegionRegistry {
 public:
  static RDMARegionRegistry& GetInstance();

  // Register [base, base+length) as one MR. Returns its rkey, or 0 when RDMA is
  // disabled (--use_rdma=false) or registration fails. Call once per arena at
  // startup; the MR is held for the process lifetime.
  uint32_t Register(void* base, size_t length);

  // rkey of the registered region fully covering [p, p+n), or 0 if none covers
  // it (signals "not registered" -> caller falls back to a copy path).
  uint32_t RkeyFor(const void* p, size_t n);

 private:
  RDMARegionRegistry() = default;

  struct Region {
    uintptr_t addr;
    size_t length;
    uint32_t rkey;
  };

  bthread::Mutex mutex_;
  std::vector<Region> regions_;
  std::vector<infiniband::MemoryRegionUPtr> owned_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_REGION_REGISTRY_H_
