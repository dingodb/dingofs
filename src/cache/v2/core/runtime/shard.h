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

#ifndef DINGOFS_CACHE_V2_CORE_RUNTIME_SHARD_H_
#define DINGOFS_CACHE_V2_CORE_RUNTIME_SHARD_H_

#include "cache/v2/core/fs/io_ring.h"
#include "cache/v2/core/reactor/poller.h"
#include "cache/v2/core/reactor/reactor.h"
#include "cache/v2/core/runtime/runtime.h"
#include "cache/v2/core/runtime/shard_inbox.h"

namespace dingofs {
namespace cache {
namespace v2 {

void BecomeShardThread(unsigned shard, int cpu);

// Everything one shard thread owns, built and torn down on that thread.
class Shard {
 public:
  Shard(unsigned id, const RuntimeOption& option, ShardInbox* inbox,
        Poller* mesh_poller);

  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  void Run(LifecycleBarrier& gate);

 private:
  void RegisterPollers();
  void UnregisterPollers();

  unsigned id_;
  ShardInbox* inbox_;    // outside -> shard
  Poller* mesh_poller_;  // peer shards -> shard; null when it has no peer
  Reactor reactor_;
  IoRing io_ring_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_RUNTIME_SHARD_H_
