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

#include "cache/v1/core/runtime/runtime.h"

#include <glog/logging.h>

#include "cache/v1/core/runtime/mesh.h"
#include "cache/v1/core/runtime/shard.h"
#include "cache/v1/core/utils/cpu.h"
#include "cache/v1/core/utils/memory/memory.h"

namespace dingofs {
namespace cache {

ShardLayout ShardLayout::Plan(const RuntimeOption& option) {
  std::vector<int> cpus = GetAllCpus();
  if (!option.cpuset.empty()) {
    auto parsed = ParseCpuSet(option.cpuset);
    CHECK(parsed.ok()) << "Fail to start runtime: "
                       << parsed.status().ToString();
    cpus = std::move(parsed).value();
  }

  const std::vector<int> cores = GetPhyCores(cpus);
  unsigned shard_count = option.shard_count != 0
                             ? option.shard_count
                             : static_cast<unsigned>(cores.size());
  if (shard_count == 0) {
    shard_count = 1;
  }

  std::vector<int> cpu_of_shard(shard_count, -1);  // -1 == unpinned
  if (option.pin_to_cpu) {
    const std::vector<int>& cpu_pool =
        (shard_count <= cores.size()) ? cores : cpus;
    for (unsigned i = 0; i < shard_count && i < cpu_pool.size(); ++i) {
      cpu_of_shard[i] = cpu_pool[i];
    }
  }
  return ShardLayout(std::move(cpu_of_shard));
}

struct SpawnWork : InboxWork {
  explicit SpawnWork(std::function<Future<>()> f) : fn(std::move(f)) {
    run = &SpawnWork::Run;
  }

  static void Run(InboxWork* base) {
    auto* self = static_cast<SpawnWork*>(base);
    (void)self->fn();
    delete self;
  }

  std::function<Future<>()> fn;
};

Runtime::Runtime(RuntimeOption option)
    : option_(std::move(option)),
      layout_(ShardLayout::Plan(option_)),
      gate_(layout_.shard_count()),
      inboxes_(std::make_unique<ShardInbox[]>(layout_.shard_count())),
      stops_(std::make_unique<StopWork[]>(layout_.shard_count())) {}

Runtime::~Runtime() {
  Stop();
  Join();
}

void Runtime::Start() {
  CHECK(state_.load(std::memory_order_relaxed) == State::kIdle)
      << "Runtime already started";

  const unsigned shard_count = layout_.shard_count();
  memory::GlobalInit(shard_count);
  Mesh::Instance().Init(shard_count);
  state_.store(State::kRunning, std::memory_order_release);

  threads_.reserve(shard_count);
  for (unsigned s = 0; s < shard_count; ++s) {
    threads_.emplace_back([this, s] {
      BecomeShardThread(s, layout_.CpuOf(s));
      Shard shard(s, option_, &inboxes_[s], Mesh::Instance().PollerFor(s));
      shard.Run(gate_);
    });
  }
  gate_.WaitAllStarted();
}

void Runtime::Stop() {
  State running = State::kRunning;
  if (!state_.compare_exchange_strong(running, State::kStopping,
                                      std::memory_order_acq_rel)) {
    return;
  }

  for (unsigned s = 0; s < layout_.shard_count(); ++s) {
    (void)inboxes_[s].Post(&stops_[s]);  // false == that shard already stopping
  }
  gate_.IssueStop();
}

void Runtime::Join() {
  const State state = state_.load(std::memory_order_acquire);
  if (state == State::kIdle || state == State::kJoined) {
    return;
  }
  CHECK(state == State::kStopping) << "Join before Stop would never return";

  for (auto& thread : threads_) {
    thread.join();
  }
  threads_.clear();
  Mesh::Instance().Destroy();
  state_.store(State::kJoined, std::memory_order_release);
}

void Runtime::SpawnOn(unsigned shard, std::function<Future<>()> func) {
  CHECK(shard < layout_.shard_count()) << "SpawnOn: shard out of range";
  auto* work = new SpawnWork(std::move(func));
  if (!inboxes_[shard].Post(work)) {
    delete work;
    LOG(FATAL) << "Fail to spawn on shard " << shard << ": it is not running";
  }
}

bool Runtime::PostTo(unsigned shard, InboxWork* work) {
  CHECK(shard < layout_.shard_count()) << "PostTo: shard out of range";
  return inboxes_[shard].Post(work);
}

}  // namespace cache
}  // namespace dingofs
