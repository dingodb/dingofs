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

#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/runtime/mesh.h"
#include "blockcache/utils/cpu.h"
#include "blockcache/utils/thread.h"

namespace dingofs {
namespace blockcache {

void BecomeShardThread(unsigned shard, int cpu) {
  SetThreadName("shard-" + std::to_string(shard));
  if (cpu >= 0) {
    PinThreadToCpu(::pthread_self(), cpu);
  }

  memory::ShardInit(shard, cpu >= 0 ? NumaNode(cpu) : -1);
}

Shard::Shard(unsigned id, ShardInbox* inbox, Poller* mesh_poller)
    : id_(id), inbox_(inbox), mesh_poller_(mesh_poller), reactor_(id) {
  Mesh::Instance().AttachReactor(id, &reactor_);
}

void Shard::Run(LifecycleBarrier& gate) {
  RegisterPollers();

  gate.WaitAllStarted();
  reactor_.Run();

  UnregisterPollers();

  if (!inbox_->empty()) {
    LOG(ERROR) << "Fail to drain shard inbox: work was posted while shard "
               << id_ << " was stopping";
  }

  gate.WaitAllStopped();
  gate.WaitStopIssued();
}

void Shard::RegisterPollers() {
  reactor_.RegisterPoller(&io_ring_);
  inbox_->Open(&reactor_);
  reactor_.RegisterPoller(inbox_);
  if (mesh_poller_ != nullptr) {
    reactor_.RegisterPoller(mesh_poller_);
  }
}

void Shard::UnregisterPollers() {
  inbox_->Close();
  if (mesh_poller_ != nullptr) {
    reactor_.UnregisterPoller(mesh_poller_);
  }
  reactor_.UnregisterPoller(inbox_);
  reactor_.UnregisterPoller(&io_ring_);
}

}  // namespace blockcache
}  // namespace dingofs
