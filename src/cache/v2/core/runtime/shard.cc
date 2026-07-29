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

#include "cache/v1/core/runtime/shard.h"

#include <glog/logging.h>
#include <pthread.h>

#include <cstdio>

#include "cache/v1/core/runtime/mesh.h"
#include "cache/v1/core/utils/cpu.h"
#include "cache/v1/core/utils/memory/memory.h"

namespace dingofs {
namespace cache {

// 1. Set thread name to "shard-<id>"
// 2. Pin thread to cpu if cpu >= 0
// 3. Initialize shard memory arena
void BecomeShardThread(unsigned shard, int cpu) {
  char name[16];
  (void)std::snprintf(name, sizeof(name), "shard-%u", shard);
  (void)::pthread_setname_np(::pthread_self(), name);
  if (cpu >= 0) {
    PinToCpu(::pthread_self(), cpu);
  }

  memory::ShardInit(shard, cpu >= 0 ? NumaNode(cpu) : -1);
}

Shard::Shard(unsigned id, const RuntimeOption& option, ShardInbox* inbox,
             Poller* mesh_poller)
    : id_(id),
      inbox_(inbox),
      mesh_poller_(mesh_poller),
      reactor_(id, option.reactor),
      io_ring_(option.io) {
  Mesh::Instance().AttachReactor(id, &reactor_);
}

// 1. Open the inbox and add pollers (inbox, mesh) to reactor
// 2. Wait for all shards to start
// 3. Start reactor (run loop)
void Shard::Run(LifecycleGate& gate) {
  RegisterPollers();

  gate.WaitAllStarted();
  reactor_.Start();  // run loop

  UnregisterPollers();

  if (!inbox_->Empty()) {
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

}  // namespace cache
}  // namespace dingofs
