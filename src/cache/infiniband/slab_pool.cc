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

#include "cache/infiniband/slab_pool.h"

#include <glog/logging.h>

#include <cstddef>
#include <mutex>

#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "common/const.h"
#include "common/options/cache.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {
namespace infiniband {

static RDMABufferPoolUPtr g_read_slab_pool;
static RDMABufferPoolUPtr g_write_slab_pool;

Status InitializeGlobalSlabPool() {
  Infiniband::Context ctx;
  auto status =
      Infiniband::Init(FLAGS_cache_rdma_device, fLU::FLAGS_cache_rdma_port_num, &ctx);
  if (!status.ok()) {
    return status;
  }

  auto* protect_domain = ctx.protect_domain;
  constexpr size_t kSlabBufferSize = 4 * kMiB;
  g_read_slab_pool =
      RDMABufferPool::Create(protect_domain, kSlabBufferSize, FLAGS_iodepth);
  g_write_slab_pool =
      RDMABufferPool::Create(protect_domain, kSlabBufferSize, FLAGS_iodepth);
  if (g_read_slab_pool == nullptr || g_write_slab_pool == nullptr) {
    return Status::OutOfMemory("create rdma slab pool failed");
  }

  return Status::OK();
}

RDMABufferPool* GetGlobalReadSlabPool() { return g_read_slab_pool.get(); }
RDMABufferPool* GetGlobalWriteSlabPool() { return g_write_slab_pool.get(); }

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
