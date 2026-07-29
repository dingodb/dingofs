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

#ifndef DINGOFS_CACHE_CORE_RUNTIME_RUNTIME_H_
#define DINGOFS_CACHE_CORE_RUNTIME_RUNTIME_H_

#include <atomic>
#include <barrier>
#include <cstdint>
#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cache/v1/core/io/io_ring.h"
#include "cache/v1/core/reactor/coroutine.h"
#include "cache/v1/core/reactor/reactor.h"
#include "cache/v1/core/runtime/shard_inbox.h"

namespace dingofs {
namespace cache {

struct RuntimeOption {
  unsigned shard_count = 0;  // 0 => one shard per physical core in cpuset
  std::string cpuset;        // "0-31,64-95"; empty => all online CPUs
  bool pin_to_cpu = true;
  ReactorOption reactor;
  IoRingOption io;
};

class ShardLayout {
 public:
  static ShardLayout Plan(const RuntimeOption& option);

  unsigned shard_count() const {
    return static_cast<unsigned>(cpu_of_shard_.size());
  }

  int CpuOf(unsigned shard) const { return cpu_of_shard_[shard]; }

 private:
  explicit ShardLayout(std::vector<int> cpu_of_shard)
      : cpu_of_shard_(std::move(cpu_of_shard)) {}

  std::vector<int> cpu_of_shard_;
};

class LifecycleGate {
 public:
  explicit LifecycleGate(unsigned shard_count)
      : all_started_(shard_count + 1),
        all_stopped_(shard_count),
        stop_issued_(1) {}

  LifecycleGate(const LifecycleGate&) = delete;
  LifecycleGate& operator=(const LifecycleGate&) = delete;

  void WaitAllStarted() { all_started_.arrive_and_wait(); }
  void WaitAllStopped() { all_stopped_.arrive_and_wait(); }
  void IssueStop() { stop_issued_.count_down(); }
  void WaitStopIssued() { stop_issued_.wait(); }

 private:
  std::barrier<> all_started_;
  std::barrier<> all_stopped_;
  std::latch stop_issued_;
};

// Stop delivered through the inbox, so a shard stops itself on its own thread.
struct StopWork : InboxWork {
  StopWork() {
    run = [](InboxWork*) { ThisReactor().Stop(); };
  }
};

template <typename Func, typename T>
Future<> InvokeIntoStdPromise(Func func,
                              std::shared_ptr<std::promise<T>> promise) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await func();
      promise->set_value();
    } else {
      promise->set_value(co_await func());
    }
  } catch (...) {
    promise->set_exception(std::current_exception());
  }
}

class Runtime {
 public:
  explicit Runtime(RuntimeOption option);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  void Start();
  void Stop();
  void Join();

  unsigned shard_count() const { return layout_.shard_count(); }

  bool PostTo(unsigned shard, InboxWork* work);

  template <typename Func>
  auto RunOn(unsigned shard, Func func) ->
      typename FutureTraits<std::invoke_result_t<Func>>::Value {
    CHECK(!HasReactor()) << "RunOn blocks a shard thread; use SubmitTo";
    using Ret = std::invoke_result_t<Func>;
    using T = typename FutureTraits<Ret>::Value;
    auto promise = std::make_shared<std::promise<T>>();
    std::future<T> fut = promise->get_future();
    SpawnOn(shard, [func = std::move(func), promise]() -> Future<> {
      return InvokeIntoStdPromise<Func, T>(func, promise);
    });
    return fut.get();
  }

 private:
  enum class State : uint8_t { kIdle, kRunning, kStopping, kJoined };

  void SpawnOn(unsigned shard, std::function<Future<>()> func);

  RuntimeOption option_;
  ShardLayout layout_;
  LifecycleGate gate_;
  std::atomic<State> state_{State::kIdle};
  std::vector<std::thread> threads_;
  std::unique_ptr<ShardInbox[]> inboxes_;
  std::unique_ptr<StopWork[]> stops_;  // one per shard, posted once
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_RUNTIME_RUNTIME_H_
