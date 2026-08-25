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

#include "blockcache/api/memory.h"

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/worker_pool.h"
#include "blockcache/infiniband/base/memory_region.h"
#include "blockcache/infiniband/client/context.h"

namespace dingofs {
namespace blockcache {

Status RegisterMemoryForRDMA(void* base, size_t bytes) {
  if (base == nullptr || bytes == 0) {
    return Status::OK();
  } else if (!FLAGS_remote_rdma || FLAGS_cache_group.empty()) {
    return Status::OK();  // no rdma, nothing to register with
  }

  CHECK(ProcessRuntimeStarted()) << "RegisterMemoryForRDMA before Start";

  return RunOnAllAndWait([base, bytes](unsigned) -> Future<Status> {
    infiniband::InfinibandContext* context =
        CHECK_NOTNULL(infiniband::ThisInfinibandContext());

    ibv_pd* pd = context->device->pd();
    StatusOr<infiniband::MemoryRegion> mr =
        co_await GetGlobalWorkers()->Submit([pd, base, bytes] {
          return infiniband::MemoryRegion::Register(pd, base, bytes);
        });
    if (!mr.ok()) {
      co_return mr.status();
    }

    context->memory_registry->Add(std::move(mr).value());
    co_return Status::OK();
  });
}

}  // namespace blockcache
}  // namespace dingofs
