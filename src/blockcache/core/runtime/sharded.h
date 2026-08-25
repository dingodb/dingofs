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

#ifndef DINGOFS_BLOCKCACHE_CORE_RUNTIME_SHARDED_H_
#define DINGOFS_BLOCKCACHE_CORE_RUNTIME_SHARDED_H_

#include <glog/logging.h>

#include <concepts>
#include <type_traits>
#include <utility>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/smp.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

// Optional shard-local lifecycle. Both are Future because tearing a shard's
// state down means draining io it already has in flight; neither is ever
// called from outside -- Sharded drives them on the owning shard.
template <typename S>
concept Startable = requires(S s) {
  { s.Start() } -> std::same_as<Future<>>;
};

template <typename S>
concept Shutdownable = requires(S s) {
  { s.Shutdown() } -> std::same_as<Future<>>;
};

// One instance of T per shard, constructed and destroyed ON its shard.
//
// This is the only place a component admits the process has shards. The
// instance is written as if it were the only one: plain members, no atomics,
// no locks, and no way to ask which shard it is -- it just uses `this`.
// Reaching a sibling is the container's job.
//
// Assembly runs on the thread that owns main(): it prepares whatever the
// instances need, then StartOnAllShards builds them, each on the core that
// will drive it, so the instance and everything its constructor allocates
// come from that core's arena.
template <typename T>
class Sharded {
 public:
  Sharded() = default;
  ~Sharded() {
    CHECK(instances_.empty()) << "Sharded destroyed before Shutdown";
  }

  Sharded(const Sharded&) = delete;
  Sharded& operator=(const Sharded&) = delete;

  // `factory` runs ON its shard and may vary per instance -- hand each shard
  // its own slice of whatever the launcher prepared. All shards build at once;
  // if any fails, none survive.
  template <typename Factory>
    requires std::is_invocable_r_v<T*, Factory, unsigned>
  Status StartOnAllShards(Factory factory) {
    CHECK(instances_.empty()) << "Sharded started twice";
    instances_.assign(ShardCount(), nullptr);

    const Status status =
        RunOnAllAndWait([this, &factory](unsigned shard) -> Future<Status> {
          return StartInstance(&instances_[shard], factory(shard));
        });
    if (!status.ok()) {
      ShutdownOnAllShards();
    }
    return status;
  }

  // Two passes on purpose: every instance stops before any is destroyed, so a
  // Shutdown() that still reaches a sibling shard finds it alive.
  void ShutdownOnAllShards() {
    if (instances_.empty()) {
      return;
    }
    RunOnAllAndWait([this](unsigned shard) -> Future<> {
      return StopInstance(instances_[shard]);
    });
    RunOnAllAndWait([this](unsigned shard) -> Future<> {
      delete std::exchange(instances_[shard], nullptr);
      return MakeReadyFuture<>();
    });
    instances_.clear();
  }

  // Fire-and-forget `func(instance)` on every shard, waiting for none of them.
  // For publishing a value the shards should pick up when they get to it -- a
  // new topology, a reloaded config -- from a thread that must not block.
  //
  // Must not race ShutdownOnAllShards: stop the publisher first, as
  // CacheGroupMemberSyncer does with its thread. Shards that stopped between
  // the post and the run are skipped; they have no use for the value.
  template <typename Func>
  void PostToAll(Func func) {
    for (unsigned shard = 0; shard < instances_.size(); ++shard) {
      auto* work = new CallWork<Func>(this, func);
      if (!PostTo(shard, work)) {
        delete work;
      }
    }
  }

  template <typename Func>
  auto InvokeOn(unsigned shard, Func func) {
    return SubmitTo(shard, [this, func = std::move(func)]() mutable {
      return func(Local());
    });
  }

  // Asks every shard for its part and folds the parts into `init`. All the
  // asking happens first, so the shards answer in parallel; the fold runs on
  // the calling shard, so `reduce` needs no synchronisation.
  template <typename Value, typename Map, typename Reduce>
  Future<Value> MapReduce(Value init, Map map, Reduce reduce) {
    using Part = typename FutureTraits<std::invoke_result_t<Map, T&>>::Value;

    std::vector<Future<Part>> parts;
    parts.reserve(instances_.size());
    for (unsigned shard = 0; shard < instances_.size(); ++shard) {
      parts.push_back(InvokeOn(shard, map));
    }
    for (Future<Part>& part : parts) {
      reduce(init, co_await std::move(part));
    }
    co_return init;
  }

  T& Local() {
    T* instance = instances_[ThisShardId()];
    CHECK(instance != nullptr) << "Sharded not started on this shard";
    return *instance;
  }

 private:
  template <typename Func>
  struct CallWork : InboxWork {
    CallWork(Sharded* owner, Func func) : owner(owner), func(std::move(func)) {
      run = [](InboxWork* base) {
        auto* self = static_cast<CallWork*>(base);
        T* instance = self->owner->instances_[ThisShardId()];
        if (instance != nullptr) {
          self->func(*instance);
        }
        delete self;
      };
    }

    Sharded* owner;
    Func func;
  };

  // Installed before Start() runs, so a failed Start still leaves the instance
  // where ShutdownOnAllShards can find and destroy it.
  static Future<Status> StartInstance(T** slot, T* instance) {
    *slot = instance;
    if constexpr (Startable<T>) {
      co_await instance->Start();
    }
    co_return Status::OK();
  }

  static Future<> StopInstance(T* instance) {
    if constexpr (Shutdownable<T>) {
      if (instance != nullptr) {
        return instance->Shutdown();
      }
    }
    return MakeReadyFuture<>();
  }

  std::vector<T*> instances_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_RUNTIME_SHARDED_H_
