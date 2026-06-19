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
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_

#include <bits/types/struct_iovec.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "cache/infiniband/common.h"
#include "cache/infiniband/infiniband.h"
#include "common/status.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class RDMABufferPool;
using RDMABufferPoolUPtr = std::unique_ptr<RDMABufferPool>;

class RDMABufferPool {
 public:
  RDMABufferPool(MemoryPoolUPtr memory_pool, MemoryRegionUPtr memory_region);
  static RDMABufferPoolUPtr Create(ProtectDomain* protect_domain,
                                   size_t buffer_size, size_t buffer_count);

  RDMABuffer* Alloc();
  void Free(RDMABuffer* buffer);
  int IndexOf(RDMABuffer* buffer);
  int IndexOf(const char* data) const;
  size_t BufferCount() { return memory_pool_->BufferCount(); }
  std::vector<iovec> Fetch();

 private:
  MemoryPoolUPtr memory_pool_;
  MemoryRegionUPtr memory_region_;
  std::vector<RDMABuffer> rdma_buffers_;
};

Status RegisterMemoryForRDMA(const std::string& device_name, void* addr,
                             size_t size);
Status DeregisterMemoryForRDMA(const std::string& device_name, void* addr);

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_
