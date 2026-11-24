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

#ifndef DINGOFS_BLOCKCACHE_CORE_RUNTIME_RUNTIME_H_
#define DINGOFS_BLOCKCACHE_CORE_RUNTIME_RUNTIME_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "blockcache/core/runtime/lifecycle_barrier.h"

namespace dingofs {
namespace blockcache {

class ShardLayout {
 public:
  // Reads --shards / --cpuset / --pin_cpu.
  static StatusOr<ShardLayout> Plan();

  int CpuOf(unsigned shard) const { return cpu_of_shard_[shard]; }
  unsigned shard_count() const {
    return static_cast<unsigned>(cpu_of_shard_.size());
  }

 private:
  explicit ShardLayout(std::vector<int> cpu_of_shard)
      : cpu_of_shard_(std::move(cpu_of_shard)) {}

  std::vector<int> cpu_of_shard_;
};

// The shard threads: start them, stop them, join them. That is all.
//
// Reaching INTO a shard is smp.h's job -- SubmitTo from another shard, PostTo
// / SpawnOn / RunOnAndWait from outside -- and none of those needs a handle.
// So nothing below the thing that owns main() has a reason to hold a Runtime,
// and no component takes one at construction.
class Runtime {
 public:
  explicit Runtime(ShardLayout layout);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  void Start();
  void Shutdown();
  void Join();

  unsigned shard_count() const { return layout_.shard_count(); }
  // Cpu shard `shard` is pinned to, -1 when unpinned.
  int CpuOf(unsigned shard) const { return layout_.CpuOf(shard); }

 private:
  enum class State : uint8_t { kIdle, kRunning, kStopping, kJoined };

  struct StopWork;  // defined in runtime.cc; one per shard, posted once

  ShardLayout layout_;
  LifecycleBarrier barrier_;
  std::atomic<State> state_{State::kIdle};
  std::vector<std::thread> threads_;
  std::unique_ptr<StopWork[]> stops_;
};

using RuntimeUPtr = std::unique_ptr<Runtime>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_RUNTIME_RUNTIME_H_
