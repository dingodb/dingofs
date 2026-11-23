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

#include "blockcache/core/runtime/shard.h"

#include <glog/logging.h>
#include <pthread.h>

#include <cstdio>
#include <optional>

#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/core/runtime/lifecycle_barrier.h"
#include "blockcache/core/runtime/mesh.h"
#include "blockcache/utils/cpu.h"
#include "blockcache/utils/thread.h"

namespace dingofs {
namespace blockcache {

void BecomeShardThread(unsigned shard, int cpu) {
  SetThreadName("shard-" + std::to_string(shard));
  if (cpu != kNoCpu) {
    BindThreadToCpu(::pthread_self(), cpu);
  }

  memory::ShardInit(shard, cpu != kNoCpu ? GetNumaNode(cpu) : kNoNumaNode);
}

Shard::Shard(unsigned id, ShardInbox* inbox, Poller* mesh_poller)
    : id_(id), inbox_(inbox), mesh_poller_(mesh_poller), reactor_(id) {
  Mesh::Instance().AttachReactor(id, &reactor_);
}

void Shard::Run(LifecycleBarrier& barrier) {
  {
    const PollerRegistration io_ring_registration(reactor_, &io_ring_);
    inbox_->Open(&reactor_);
    const PollerRegistration inbox_registration(reactor_, inbox_);
    std::optional<PollerRegistration> mesh_registration;
    if (mesh_poller_ != nullptr) {
      mesh_registration.emplace(reactor_, mesh_poller_);
    }

    barrier.WaitAllStarted();
    reactor_.Run();
    inbox_->Close();
  }

  if (!inbox_->empty()) {
    LOG(ERROR) << "Fail to drain shard inbox: work was posted while shard "
               << id_ << " was stopping";
  }

  barrier.WaitAllStopped();
  barrier.WaitStopIssued();
}

}  // namespace blockcache
}  // namespace dingofs
