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

#include "blockcache/core/runtime/bootstrap.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "blockcache/core/filesystem/io_ring.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/memory/slab_pool.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/runtime.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/thread_pool.h"
#include "blockcache/utils/align.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint64(buffer_pool_mb, 256, "buffer pool per shard in MiB");

static RuntimeUPtr g_runtime;
static ThreadPoolUPtr g_thread_pool;

static Future<Status> OpenBufferPool(size_t superblocks) {
  const Status status = BufferPool::Open(superblocks);
  if (!status.ok()) {
    co_return status;
  }

  if (HasIoRing() && !ThisIoRing().buffers().registered()) {
    const SlabPool* pool = BufferPool::LocalPool();
    const int rc = ThisIoRing().buffers().Register(
        pool->base(), pool->total_bytes(), SlabPool::kSuperblockSize);
    if (rc < 0) {
      LOG(WARNING) << "Fail to register the buffer pool with io_uring,"
                      " falling back to unfixed io: rc="
                   << rc;
    }
  }
  co_return Status::OK();
}

static Future<> CloseBufferPool() {
  if (BufferPool::LocalPool() != nullptr && HasIoRing() &&
      ThisIoRing().buffers().registered()) {
    ThisIoRing().buffers().Unregister();
  }
  BufferPool::Close();
  co_return;
}

Status StartProcessRuntime() {
  CHECK(g_runtime == nullptr) << "the process runtime is already up";

  StatusOr<ShardLayout> layout = ShardLayout::Plan();
  if (!layout.ok()) {
    return layout.status();
  }
  g_runtime = std::make_unique<Runtime>(std::move(layout).value());
  g_runtime->Start();

  const size_t superblocks =
      (size_t{FLAGS_buffer_pool_mb} * kMiB + SlabPool::kSuperblockSize - 1) /
      SlabPool::kSuperblockSize;
  const Status status =
      RunOnAllAndWait([superblocks](unsigned /*shard*/) -> Future<Status> {
        return OpenBufferPool(superblocks);
      });
  if (!status.ok()) {
    StopProcessRuntime();
    return status;
  }

  std::vector<int> shard_cpus(g_runtime->shard_count());
  for (unsigned shard = 0; shard < shard_cpus.size(); ++shard) {
    shard_cpus[shard] = g_runtime->CpuOf(shard);
  }
  auto pool = std::make_unique<ThreadPool>();
  pool->Start(std::move(shard_cpus));
  g_thread_pool = std::move(pool);
  return Status::OK();
}

void StopProcessRuntime() {
  if (g_runtime == nullptr) {
    return;
  }

  if (g_thread_pool != nullptr) {
    g_thread_pool->Shutdown();
    g_thread_pool.reset();
  }

  RunOnAllAndWait(
      [](unsigned /*shard*/) -> Future<> { return CloseBufferPool(); });

  g_runtime->Shutdown();
  g_runtime->Join();
  g_runtime.reset();
}

bool ProcessRuntimeStarted() { return g_runtime != nullptr; }

ThreadPool* GetGlobalThreadPool() { return g_thread_pool.get(); }

}  // namespace blockcache
}  // namespace dingofs
