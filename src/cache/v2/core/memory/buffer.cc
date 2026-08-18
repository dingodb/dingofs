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

#include "cache/v2/core/memory/buffer.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>

#include "cache/v2/core/fs/io_ring.h"
#include "cache/v2/core/memory/shard_allocator.h"
#include "cache/v2/core/reactor/reactor.h"
#include "cache/v2/core/runtime/smp.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_uint64(buffer_pool_mb, 256, "buffer pool per shard in MiB");

// One per shard, created on the shard thread so its pages are NUMA-local.
thread_local std::unique_ptr<SlabPool> tls_pool;

Buffer Buffer::Alloc(size_t n) {
  SlabPool* pool = tls_pool.get();
  if (pool == nullptr || n == 0) {
    return {};
  }
  char* data = pool->Alloc(n);
  if (data == nullptr) {
    // No heap fallback on purpose: the device rejects unregistered memory.
    return {};
  }
  return Buffer(data, n);
}

void Buffer::Reset() noexcept {
  if (base_ != nullptr) {
    SlabPool* pool = tls_pool.get();
    DCHECK(pool != nullptr) << "a Buffer outlived its shard's pool";
    if (pool != nullptr) {
      pool->Free(base_);  // PopFront moved data_; the pool knows only this one
    }
    base_ = nullptr;
    data_ = nullptr;
    size_ = 0;
  }
}

static Future<Status> OpenPoolOnThisShard(size_t superblocks) {
  if (tls_pool != nullptr) {
    co_return Status::OK();  // idempotent
  }
  SlabPool::Option option;
  option.superblock_count = superblocks;
  option.numa_node = memory::LocalNumaNode();
  auto pool = std::make_unique<SlabPool>(option);

  // chunk == superblock: any allocation sits in one registered buffer.
  if (HasIoRing()) {
    const int rc = ThisIoRing().buffers().Register(
        pool->base(), pool->total_bytes(), SlabPool::kSuperblockSize);
    if (rc < 0) {
      // Not fatal: fixed buffers are only an optimisation.
      LOG(WARNING) << "Fail to register the buffer pool with io_uring,"
                      " falling back to unfixed io: rc="
                   << rc;
    }
  }
  tls_pool = std::move(pool);
  co_return Status::OK();
}

static Future<> ClosePoolOnThisShard() {
  if (tls_pool != nullptr && HasIoRing() &&
      ThisIoRing().buffers().registered()) {
    ThisIoRing().buffers().Unregister();
  }
  tls_pool.reset();
  co_return;
}

Status BufferPool::InitOnAllShards(size_t bytes_per_shard) {
  const size_t superblocks = (bytes_per_shard + SlabPool::kSuperblockSize - 1) /
                             SlabPool::kSuperblockSize;
  if (superblocks == 0) {
    return Status::InvalidParam(
        "buffer pool must hold at least one 4MiB superblock");
  }

  // Every shard reserves and pins its own pages; doing that one shard at a
  // time is the bulk of a large pool's startup.
  return RunOnAllAndWait([superblocks](unsigned /*shard*/) -> Future<Status> {
    return OpenPoolOnThisShard(superblocks);
  });
}

void BufferPool::ShutdownOnAllShards() {
  RunOnAllAndWait(
      [](unsigned /*shard*/) -> Future<> { return ClosePoolOnThisShard(); });
}

SlabPool* BufferPool::LocalPool() { return tls_pool.get(); }

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
