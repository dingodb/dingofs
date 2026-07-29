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

#ifndef DINGOFS_CACHE_CORE_RUNTIME_SHARDED_H_
#define DINGOFS_CACHE_CORE_RUNTIME_SHARDED_H_

#include <glog/logging.h>

#include <concepts>
#include <exception>
#include <utility>
#include <vector>

#include "cache/v1/core/reactor/coroutine.h"
#include "cache/v1/core/runtime/smp.h"

namespace dingofs {
namespace cache {

template <typename S>
concept Stoppable = requires(S s) {
  { s.Stop() } -> std::same_as<Future<>>;
};

// One Service instance per shard, constructed and destroyed ON its shard so
// its memory and resources are shard-local.
template <typename Service>
class Sharded {
 public:
  Sharded() = default;
  ~Sharded() { CHECK(instances_.empty()) << "Sharded destroyed before Stop"; }

  Sharded(const Sharded&) = delete;
  Sharded& operator=(const Sharded&) = delete;

  template <typename... Args>
  Future<> Start(Args... args) {
    instances_.assign(ShardCount(), nullptr);
    std::vector<Future<>> starting;
    starting.reserve(instances_.size());
    for (unsigned s = 0; s < instances_.size(); ++s) {
      starting.push_back(SubmitTo(
          s, [this, s, args...] { instances_[s] = new Service(args...); }));
    }

    std::exception_ptr failure;
    try {
      co_await WhenAll(std::move(starting));
    } catch (...) {
      failure = std::current_exception();
    }
    if (failure) {
      co_await Stop();
      std::rethrow_exception(failure);
    }
  }

  Future<> Stop() {
    std::vector<Future<>> stopping;
    stopping.reserve(instances_.size());
    for (unsigned s = 0; s < instances_.size(); ++s) {
      stopping.push_back(SubmitTo(s, [this, s]() -> Future<> {
        return StopAndDelete(std::exchange(instances_[s], nullptr));
      }));
    }
    co_await WhenAll(std::move(stopping));
    instances_.clear();
  }

  Service& Local() {
    Service* service = instances_[ThisShardId()];
    CHECK(service != nullptr) << "Sharded not started on this shard";
    return *service;
  }

  template <typename Func>
  auto InvokeOn(unsigned shard, Func func) {
    return SubmitTo(shard, [this, func = std::move(func)]() mutable {
      return func(Local());
    });
  }

  template <typename Func>
  Future<> InvokeOnAll(Func func) {
    std::vector<Future<>> running;
    running.reserve(ShardCount());
    for (unsigned s = 0; s < ShardCount(); ++s) {
      running.push_back(InvokeOn(s, func));
    }
    co_await WhenAll(std::move(running));
  }

 private:
  static Future<> StopAndDelete(Service* service) {
    if (service != nullptr) {
      if constexpr (Stoppable<Service>) {
        co_await service->Stop();
      }
      delete service;
    }
  }

  std::vector<Service*> instances_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_RUNTIME_SHARDED_H_
