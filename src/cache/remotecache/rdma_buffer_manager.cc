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
 * Created Date: 2026-05-31
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/rdma_buffer_manager.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cache/infiniband/infiniband.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(cache_rdma_client_pool_size, 1024,
              "number of buffers in the client RDMA buffer pool; bounds the "
              "in-flight client RDMA Range/Put/Cache concurrency");
DEFINE_uint32(
    cache_rdma_client_pool_buffer_size, 4 * 1024 * 1024,
    "size of each client RDMA buffer in bytes; must be >= the largest "
    "block size transferred");

RDMABufferManager& RDMABufferManager::GetInstance() {
  static RDMABufferManager instance;
  return instance;
}

infiniband::RDMABufferPool* RDMABufferManager::Pool() {
  std::call_once(once_, [this]() {
    infiniband::Infiniband::Context ctx;
    auto status = infiniband::Infiniband::Init(
        FLAGS_cache_rdma_device,
        static_cast<uint8_t>(FLAGS_cache_rdma_port_num), &ctx);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to init infiniband for client RDMA pool: "
                 << status.ToString();
      return;
    }
    pool_ = infiniband::RDMABufferPool::Create(
        ctx.protect_domain, FLAGS_cache_rdma_client_pool_buffer_size,
        FLAGS_cache_rdma_client_pool_size);
    if (pool_ == nullptr) {
      LOG(ERROR) << "Fail to create client RDMA buffer pool";
    }
  });
  return pool_.get();
}

bool RDMABufferManager::Enabled() {
  return FLAGS_use_rdma && Pool() != nullptr;
}

IOBuffer RDMABufferManager::NewBuffer(size_t size) {
  auto* pool = Pool();
  if (pool == nullptr) {
    return IOBuffer();
  }
  auto* buffer = pool->Alloc();
  if (buffer == nullptr || size > buffer->capacity) {
    if (buffer != nullptr) {
      pool->Free(buffer);
    }
    return IOBuffer();
  }
  IOBuffer out;
  out.AppendUserDataWithMeta(
      buffer->data, size, [pool, buffer](void*) { pool->Free(buffer); },
      buffer->rkey);
  return out;
}

IOBuffer RDMABufferManager::EnsureRegistered(const IOBuffer& src) {
  if (src.Size() == 0) {
    return src;
  }
  // Already a single registered block (meta carries the rkey) -> zero copy.
  auto& mutable_src = const_cast<IOBuffer&>(src);
  if (mutable_src.ConstIOBuf().backing_block_num() == 1 &&
      mutable_src.GetFirstDataMeta() != 0) {
    return src;
  }
  // Otherwise stage it into a registered buffer (single copy).
  IOBuffer registered = NewBuffer(src.Size());
  if (registered.Size() == 0) {
    return src;  // pool exhausted; advertise will fail cleanly downstream
  }
  src.CopyTo(registered.Fetch1(), src.Size());
  return registered;
}

}  // namespace cache
}  // namespace dingofs
