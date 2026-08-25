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

// Transport-neutral: how a coroutine waits for one completion. io_uring
// (core/fs) and verbs (infiniband/base) both derive from it.

#ifndef DINGOFS_BLOCKCACHE_CORE_REACTOR_IO_AWAITER_H_
#define DINGOFS_BLOCKCACHE_CORE_REACTOR_IO_AWAITER_H_

#include <coroutine>
#include <cstdint>

#include "blockcache/core/reactor/reactor.h"  // Schedule, Task, TaskPromise

namespace dingofs {
namespace blockcache {

class IoCompletion {
 public:
  virtual void Complete(int32_t res) noexcept = 0;

 protected:
  ~IoCompletion() = default;
};

template <typename Derived>
class IoAwaiter {
 public:
  bool await_ready() const noexcept { return false; }

  template <TaskPromise P>
  void await_suspend(std::coroutine_handle<P> h) noexcept {
    continuation_ = &h.promise();
    static_cast<Derived*>(this)->Arm();
  }

 protected:
  ~IoAwaiter() = default;
  IoAwaiter() = default;

  IoAwaiter(const IoAwaiter&) = delete;
  IoAwaiter& operator=(const IoAwaiter&) = delete;

  void ResumeLater(int32_t result) noexcept {
    result_ = result;
    static_cast<Derived*>(this)->OnResult();
    Schedule(continuation_);
  }

  void OnResult() noexcept {}

  int32_t result_ = 0;
  Task* continuation_ = nullptr;  // the coroutine waiting on it
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_REACTOR_IO_AWAITER_H_
