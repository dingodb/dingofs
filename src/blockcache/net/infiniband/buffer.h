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

#ifndef DINGOFS_BLOCKCACHE_NET_INFINIBAND_BUFFER_H_
#define DINGOFS_BLOCKCACHE_NET_INFINIBAND_BUFFER_H_

#include <cstdint>

namespace dingofs {
namespace blockcache {
namespace verbs {

// A local, registered buffer: what goes into an ibv_sge.
struct LocalBuf {
  void* addr = nullptr;
  uint32_t len = 0;
  uint32_t lkey = 0;
};

// A remote, registered range a peer publishes for one-sided RDMA.
struct RemoteBuf {
  uint64_t addr = 0;
  uint64_t len = 0;
  uint32_t rkey = 0;

  bool Valid() const { return addr != 0 && len != 0; }
  // Sub-range at `offset`, for splitting one region across transfers.
  RemoteBuf Slice(uint64_t offset, uint64_t n) const {
    return RemoteBuf{.addr = addr + offset, .len = n, .rkey = rkey};
  }
};

}  // namespace verbs
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_INFINIBAND_BUFFER_H_
