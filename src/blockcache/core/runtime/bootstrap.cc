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

#include <glog/logging.h>

#include <memory>
#include <utility>
#include <vector>

#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/runtime/runtime.h"
#include "blockcache/core/runtime/worker_pool.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

static RuntimeUPtr g_runtime;
static WorkerPoolUPtr g_worker_pool;

void StartProcessRuntime() {
  CHECK(g_runtime == nullptr) << "the process runtime is already up";

  g_runtime = std::make_unique<Runtime>();
  g_runtime->Start();

  const Status status = BufferPool::InitOnAllShards();
  CHECK(status.ok()) << "Fail to create the buffer pools: "
                     << status.ToString();

  std::vector<int> shard_cpus(g_runtime->shard_count());
  for (unsigned shard = 0; shard < shard_cpus.size(); ++shard) {
    shard_cpus[shard] = g_runtime->CpuOf(shard);
  }
  g_worker_pool = std::make_unique<WorkerPool>();
  g_worker_pool->Start(std::move(shard_cpus));
}

void StopProcessRuntime() {
  if (g_runtime == nullptr) {
    return;
  }

  g_worker_pool->Shutdown();
  g_worker_pool.reset();

  BufferPool::ShutdownOnAllShards();

  g_runtime->Shutdown();
  g_runtime->Join();
  g_runtime.reset();
}

bool ProcessRuntimeStarted() { return g_runtime != nullptr; }

}  // namespace blockcache
}  // namespace dingofs
