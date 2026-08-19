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

#include "blockcache/core/runtime/runtime.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/runtime/mesh.h"
#include "blockcache/core/runtime/shard.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/utils/cpu.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(shards, 0, "reactor shards, 0 means one per core");
DEFINE_string(cpuset, "", "cores the shards run on, e.g. 0-31");
DEFINE_bool(pin_cpu, false, "pin each shard to its core");
DEFINE_bool(poll_mode, false, "busy-poll the reactors");

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

// Stop is delivered through the inbox, so a shard stops itself on its own
// thread rather than being stopped from under one.
struct Runtime::StopWork : InboxWork {
  StopWork() {
    run = [](InboxWork*) { ThisReactor().Shutdown(); };
  }
};

Runtime::Runtime(RuntimeOption option)
    : option_(std::move(option)),
      layout_(ShardLayout::Plan(option_)),
      gate_(layout_.shard_count()),
      stops_(std::make_unique<StopWork[]>(layout_.shard_count())) {}

Runtime::~Runtime() {
  Shutdown();
  Join();
}

void Runtime::Start() {
  CHECK(state_.load(std::memory_order_relaxed) == State::kIdle)
      << "Runtime already started";

  LOG(INFO) << "Runtime is starting...";

  const unsigned shard_count = layout_.shard_count();
  memory::GlobalInit(shard_count);
  Mesh::Instance().Init(shard_count);
  state_.store(State::kRunning, std::memory_order_release);

  threads_.reserve(shard_count);
  for (unsigned s = 0; s < shard_count; ++s) {
    threads_.emplace_back([this, s] {
      BecomeShardThread(s, layout_.CpuOf(s));
      Mesh& mesh = Mesh::Instance();
      Shard shard(s, option_, &mesh.InboxFor(s), mesh.PollerFor(s));
      shard.Run(gate_);
    });
  }
  gate_.WaitAllStarted();

  LOG(INFO) << "Successfully start Runtime: shards=" << shard_count;
}

void Runtime::Shutdown() {
  State running = State::kRunning;
  if (!state_.compare_exchange_strong(running, State::kStopping,
                                      std::memory_order_acq_rel)) {
    return;
  }

  LOG(INFO) << "Runtime is shutting down...";

  for (unsigned s = 0; s < layout_.shard_count(); ++s) {
    (void)PostTo(s, &stops_[s]);  // false == that shard already stopping
  }
  gate_.IssueStop();

  LOG(INFO) << "Successfully shutdown Runtime";
}

void Runtime::Join() {
  const State state = state_.load(std::memory_order_acquire);
  if (state == State::kIdle || state == State::kJoined) {
    return;
  }
  CHECK(state == State::kStopping) << "Join before Shutdown would never return";

  for (auto& thread : threads_) {
    thread.join();
  }
  threads_.clear();
  Mesh::Instance().Destroy();
  state_.store(State::kJoined, std::memory_order_release);
}

}  // namespace blockcache
}  // namespace dingofs
