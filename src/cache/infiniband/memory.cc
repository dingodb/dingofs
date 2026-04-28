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

#include "cache/infiniband/memory.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <memory>
#include <unordered_map>

#include "cache/infiniband/infiniband.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

RdmaBufferPool::RdmaBufferPool(MemoryPoolUPtr memory_pool,
                               MemoryRegionUPtr memory_region)
    : memory_pool_(std::move(memory_pool)),
      memory_region_(std::move(memory_region)) {
  size_t buffer_size = memory_pool_->buffer_size();
  size_t buffer_count = memory_pool_->buffer_count();
  rdma_buffers_.reserve(buffer_count);
  for (int i = 0; i < buffer_count; ++i) {
    RdmaBuffer buffer;
    buffer.data = memory_pool_->base() + (i * buffer_size);
    buffer.capacity = static_cast<uint32_t>(buffer_size);
    buffer.lkey = memory_region_->GetLkey();
    buffer.rkey = memory_region_->GetRkey();
    buffer.index = i;
    rdma_buffers_.emplace_back(buffer);
  }
}

RdmaBufferPoolUPtr RdmaBufferPool::Create(ProtectDomain* protect_domain,
                                          size_t buffer_size,
                                          size_t buffer_count) {
  CHECK_NOTNULL(protect_domain);

  auto memory_pool = MemoryPool::Create(buffer_size, buffer_count);
  if (memory_pool == nullptr) {
    return nullptr;
  }

  auto memory_region = MemoryRegion::Register(
      protect_domain, memory_pool->base(), buffer_size * buffer_count);
  if (memory_region == nullptr) {
    return nullptr;
  }

  LOG(INFO) << "Successfully create RdmaBufferPool{buffer_size=" << buffer_size
            << " buffer_count=" << buffer_count
            << " lkey=" << memory_region->GetLkey()
            << " rkey=" << memory_region->GetRkey() << "}";

  return std::make_unique<RdmaBufferPool>(std::move(memory_pool),
                                          std::move(memory_region));
}

RdmaBuffer* RdmaBufferPool::Alloc() {
  char* addr = memory_pool_->Require();
  if (addr == nullptr) {
    return nullptr;
  }
  return &rdma_buffers_[memory_pool_->IndexOf(addr)];
}

void RdmaBufferPool::Free(RdmaBuffer* rdma_buffer) {
  DCHECK(rdma_buffer != nullptr);
  DCHECK_GE(rdma_buffer, rdma_buffers_.data());
  DCHECK_LT(rdma_buffer, rdma_buffers_.data() + rdma_buffers_.size());
  memory_pool_->Release(rdma_buffer->data);
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
