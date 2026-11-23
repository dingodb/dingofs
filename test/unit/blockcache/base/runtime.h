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

/*
 * Project: DingoFS
 * Created Date: 2026-09-23
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_RUNTIME_H_
#define DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_RUNTIME_H_

#include <gtest/gtest.h>

#include <chrono>
#include <source_location>
#include <thread>
#include <type_traits>
#include <utility>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/core/runtime/smp.h"
#include "common/options/cache.h"

namespace dingofs {
namespace blockcache {

// The shard count of every runtime a test brings up: the shard arena is
// mapped once per process (memory::GlobalInit) and refuses a different count
// afterwards.
constexpr unsigned kShards = 2;

// The flags of every runtime a test brings up, through a ScopedRuntime or
// through a component that starts its own (BlockCacheImpl).
inline void SetRuntimeFlags() {
  FLAGS_shards = kShards;
  FLAGS_buffer_pool_mb = 4;  // one superblock per shard is plenty here
  FLAGS_offload_threads = 4;
}

// Keeps the process runtime (shard reactors, buffer pools and the offload
// thread pool) up for as long as it lives, so every test gets a fresh one.
// Declare it as a fixture's first member, after any FlagSaver, so it starts
// before and stops after everything else the test builds; or as a local in
// the one test that needs it.
class ScopedRuntime {
 public:
  ScopedRuntime() {
    SetRuntimeFlags();
    StartProcessRuntime();
  }

  ~ScopedRuntime() { StopProcessRuntime(); }

  ScopedRuntime(const ScopedRuntime&) = delete;
  ScopedRuntime& operator=(const ScopedRuntime&) = delete;
};

// Runs `func` (returning Future<T>) on `shard` and blocks until it resolves;
// returns the T.
template <typename Func>
auto RunOnShard(unsigned shard, Func func) {
  return RunOnAndWait(shard, std::move(func));
}

// Evaluates `pred` on `shard` every 10ms until it holds or `timeout` passes;
// returns whether it held. For state that a background coroutine changes on
// its own schedule.
template <typename Pred>
bool WaitUntil(unsigned shard, Pred pred,
               std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  const auto holds = [&pred] { return MakeReadyFuture<bool>(pred()); };
  while (!RunOnShard(shard, holds)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}

// Calls `fn(args...)` on `shard` and blocks until it returns, or until the
// future it returns resolves; returns that value. Unlike RunOnShard's, `fn`
// may return a plain value or nothing. `fn` takes its arguments by value.
template <typename Fn, typename... Args>
auto CallOn(unsigned shard, Fn fn, Args... args) {
  using Result = std::invoke_result_t<Fn&, Args&...>;
  return RunOnShard(shard, [&fn, &args...]() {
    if constexpr (FutureTraits<Result>::kIsFuture) {
      return fn(args...);
    } else if constexpr (std::is_void_v<Result>) {
      fn(args...);
      return MakeReadyFuture<>();
    } else {
      return MakeReadyFuture<Result>(fn(args...));
    }
  });
}

// Awaits `future`, which the unit under test must have resolved already: it
// wakes a parked waiter with one Schedule, so one Yield lets it run. A future
// still pending then fails the test at the caller's line and yields T{}
// instead of hanging it.
template <typename T>
Future<T> AwaitReady(
    Future<T> future,
    std::source_location caller = std::source_location::current()) {
  co_await Yield();
  if (!future.Available()) {
    ADD_FAILURE_AT(caller.file_name(), static_cast<int>(caller.line()))
        << "a parked waiter was never woken";
    if constexpr (std::is_void_v<T>) {
      co_return;
    } else {
      co_return T{};
    }
  }
  co_return co_await std::move(future);
}

// Yields on this shard until `pred` holds or `timeout` passes; returns
// whether it held. For progress the shard makes between task batches (rdma
// completions it reaps, work requests it posts). While it loops the shard's
// task queue never empties, so its pollers run only at preemption ticks
// (--task_quota_us): no clock for perf tests.
template <typename Pred>
Future<bool> YieldUntil(
    Pred pred, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      co_return false;
    }
    co_await Yield();
  }
  co_return true;
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_RUNTIME_H_
