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

#ifndef DINGOFS_BLOCKCACHE_CORE_RUNTIME_SMP_H_
#define DINGOFS_BLOCKCACHE_CORE_RUNTIME_SMP_H_

#include <glog/logging.h>

#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/mesh.h"
#include "common/status.h"

// Reaching a shard: from another shard (SubmitTo, below) or from a thread
// outside the runtime (PostTo / SpawnOn / RunOnAndWait, at the bottom).
//
// None of them takes a Runtime handle. A process has exactly one runtime and
// Mesh owns the per-shard table, so `Runtime` stays what it is -- the object
// that starts and stops the shard threads -- instead of becoming a handle
// every component has to be handed at construction.

namespace dingofs {
namespace blockcache {

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

  Future<Value> GetFuture() { return promise_.GetFuture(); }

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

// Hands `work` to the shard through its inbox; false when that shard is not
// running, including when no runtime is up at all. On false the caller still
// owns `work`. On true it runs on the shard and owns itself from there.
inline bool PostTo(unsigned shard, InboxWork* work) {
  Mesh& mesh = Mesh::Instance();
  if (!mesh.built()) {
    return false;
  }
  CHECK(shard < mesh.shard_count()) << "PostTo: shard out of range";
  return mesh.InboxFor(shard).Post(work);
}

struct SpawnWork : InboxWork {
  explicit SpawnWork(std::function<Future<>()> f) : fn(std::move(f)) {
    run = &SpawnWork::Run;
  }

  static void Run(InboxWork* base) {
    auto* self = static_cast<SpawnWork*>(base);
    (void)self->fn();  // detached; the coroutine owns itself
    delete self;
  }

  std::function<Future<>()> fn;
};

// A detached coroutine on `shard`. Fatal if the shard is not running: the
// caller keeps no handle, so a silent drop would strand whatever it captured.
inline void SpawnOn(unsigned shard, std::function<Future<>()> func) {
  auto* work = new SpawnWork(std::move(func));
  if (!PostTo(shard, work)) {
    delete work;
    LOG(FATAL) << "Fail to spawn on shard " << shard << ": it is not running";
  }
}

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

// Runs `func` on `shard` and BLOCKS until it resolves. External threads only:
// on a shard it would park the very thread that has to run the work.
template <typename Func>
auto RunOnAndWait(unsigned shard, Func func) ->
    typename FutureTraits<std::invoke_result_t<Func>>::Value {
  CHECK(!HasReactor()) << "RunOnAndWait blocks a shard thread; use SubmitTo";
  using Ret = std::invoke_result_t<Func>;
  using T = typename FutureTraits<Ret>::Value;
  auto promise = std::make_shared<std::promise<T>>();
  std::future<T> fut = promise->get_future();
  SpawnOn(shard, [func = std::move(func), promise]() -> Future<> {
    return InvokeIntoStdPromise<Func, T>(func, promise);
  });
  return fut.get();
}

template <typename T, typename Func>
Future<> InvokeIntoLatch(Func* func, unsigned shard, Status* failed,
                         std::latch* done) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await (*func)(shard);
    } else {
      *failed = co_await (*func)(shard);
    }
  } catch (const std::exception& e) {
    *failed = Status::Internal(e.what());
  } catch (...) {
    *failed = Status::Internal("unknown exception");
  }
  done->count_down();
}

// Runs `func(shard)` on every shard AT ONCE and blocks until all of them
// resolve. External threads only, for the same reason as RunOnAndWait.
//
// Every shard runs even when an earlier one fails -- teardown has to reach all
// of them either way, and a half-built Sharded still has to be torn down. With
// `func` returning Future<Status>, the first failure in shard order is the
// result; with Future<>, failures are logged and the call still waits for all.
template <typename Func>
auto RunOnAllAndWait(Func func) ->
    typename FutureTraits<std::invoke_result_t<Func, unsigned>>::Value {
  CHECK(!HasReactor()) << "RunOnAllAndWait blocks a shard thread; use SubmitTo";
  using Ret = std::invoke_result_t<Func, unsigned>;
  using T = typename FutureTraits<Ret>::Value;
  static_assert(FutureTraits<Ret>::kIsFuture &&
                    (std::is_void_v<T> || std::is_same_v<T, Status>),
                "RunOnAllAndWait: func returns Future<> or Future<Status>");

  const unsigned shards = ShardCount();
  std::vector<Status> failed(shards);
  std::latch done(shards);
  for (unsigned s = 0; s < shards; ++s) {
    SpawnOn(s, [&func, failed = &failed[s], done = &done, s]() -> Future<> {
      return InvokeIntoLatch<T>(&func, s, failed, done);
    });
  }
  done.wait();

  if constexpr (std::is_void_v<T>) {
    for (unsigned s = 0; s < shards; ++s) {
      LOG_IF(ERROR, !failed[s].ok())
          << "Fail to run on shard " << s << ": " << failed[s].ToString();
    }
  } else {
    for (const Status& status : failed) {
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_RUNTIME_SMP_H_
