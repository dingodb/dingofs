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

#ifndef DINGOFS_CACHE_CORE_RUNTIME_SMP_H_
#define DINGOFS_CACHE_CORE_RUNTIME_SMP_H_

#include <type_traits>
#include <utility>
#include <vector>

#include "cache/v1/core/reactor/coroutine.h"
#include "cache/v1/core/runtime/mesh.h"

// Running work on another shard and getting the answer back as a Future;
// runtime/mesh.h carries the items.

namespace dingofs {
namespace cache {

inline unsigned ShardCount() { return Mesh::Instance().shard_count(); }

template <typename T, typename Func>
void InvokeIntoState(FutureState<T>& out, Func& func) noexcept {
  try {
    if constexpr (std::is_void_v<T>) {
      func();
      out.SetValue();
    } else {
      out.SetValue(func());
    }
  } catch (...) {
    out.SetException(std::current_exception());
  }
}

template <typename T, typename Func>
Future<T> InvokeToFuture(Func& func) {
  try {
    if constexpr (std::is_void_v<T>) {
      func();
      return MakeReadyFuture<>();
    } else {
      return MakeReadyFuture<T>(func());
    }
  } catch (...) {
    return MakeExceptionFuture<T>(std::current_exception());
  }
}

template <typename Func>
class SubmitWork final : public MeshWork {
 public:
  using Ret = std::invoke_result_t<Func>;
  using Value = typename FutureTraits<Ret>::Value;

  SubmitWork(Func func, MeshLink* home) : func_(std::move(func)), home_(home) {}

  Future<Value> GetFuture() { return promise_.GetFuture(); }

  void Run() noexcept override {
    if constexpr (FutureTraits<Ret>::kIsFuture) {
      (void)AwaitAndRespond(this);
    } else {
      InvokeIntoState(result_, func_);
      home_->Respond(this);
    }
  }

  void OnComplete() noexcept override {
    promise_.SetFrom(std::move(result_));
    delete this;
  }

 private:
  static Future<> AwaitAndRespond(SubmitWork* self) {
    try {
      if constexpr (std::is_void_v<Value>) {
        co_await self->func_();
        self->result_.SetValue();
      } else {
        self->result_.SetValue(co_await self->func_());
      }
    } catch (...) {
      self->result_.SetException(std::current_exception());
    }
    self->home_->Respond(self);
  }

  Func func_;
  FutureState<Value> result_;  // written on callee, read on caller (ring syncs)
  Promise<Value> promise_;     // caller-shard only
  MeshLink* home_;             // the callee's link back to the caller
};

template <typename Func>
auto SubmitTo(unsigned shard, Func&& func) -> Future<
    typename FutureTraits<std::invoke_result_t<std::decay_t<Func>>>::Value> {
  using Decayed = std::decay_t<Func>;
  using Ret = std::invoke_result_t<Decayed>;
  using Value = typename FutureTraits<Ret>::Value;

  if (shard == ThisShardId()) {
    if constexpr (FutureTraits<Ret>::kIsFuture) {
      return func();
    } else {
      return InvokeToFuture<Value>(func);
    }
  }

  Mesh& mesh = Mesh::Instance();
  DCHECK(shard < mesh.shard_count()) << "SubmitTo: shard out of range";
  const unsigned me = ThisShardId();

  MeshLink& home = mesh.LinkOf(shard, me);
  auto* work = new SubmitWork<Decayed>(std::forward<Func>(func), &home);
  Future<Value> future = work->GetFuture();
  MeshLink& outbound = mesh.LinkOf(me, shard);
  outbound.Submit(work);
  return future;
}

template <typename Func>
Future<> InvokeOnAll(Func func) {
  std::vector<Future<>> futures;
  futures.reserve(ShardCount());
  for (unsigned s = 0; s < ShardCount(); ++s) {
    futures.push_back(SubmitTo(s, func));
  }
  co_await WhenAll(std::move(futures));
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_RUNTIME_SMP_H_
