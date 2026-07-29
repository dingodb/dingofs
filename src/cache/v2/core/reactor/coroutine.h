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

#ifndef DINGOFS_CACHE_CORE_REACTOR_COROUTINE_H_
#define DINGOFS_CACHE_CORE_REACTOR_COROUTINE_H_

#include <glog/logging.h>

#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#include "cache/v1/core/reactor/preempt.h"
#include "cache/v1/core/reactor/reactor.h"
#include "cache/v1/core/reactor/task.h"
#include "cache/v1/core/reactor/timer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

template <typename T = void>
class Future;
template <typename T = void>
class Promise;
template <typename T>
class FutureAwaiter;
template <typename T>
class CoroutinePromise;

template <typename T = void, typename... Args>
Future<T> MakeReadyFuture(Args&&... args);
template <typename T = void>
Future<T> MakeExceptionFuture(std::exception_ptr ex);

class BrokenPromise : public std::exception {
 public:
  const char* what() const noexcept override { return "broken promise"; }
};

[[gnu::noinline, gnu::cold]] inline std::exception_ptr MakeBrokenPromise() {
  return std::make_exception_ptr(BrokenPromise{});
}

[[gnu::noinline, gnu::cold]] inline void ReportLostStatus(
    const Status& status) {
  LOG(ERROR) << "Discarded result holds a failed status: " << status.ToString();
}

[[gnu::noinline, gnu::cold, noreturn]] inline void ReportLostException(
    std::exception_ptr ex) {
  try {
    std::rethrow_exception(std::move(ex));
  } catch (const std::exception& e) {
    LOG(FATAL) << "Discarded result holds an unhandled exception: " << e.what();
  } catch (...) {
    LOG(FATAL)
        << "Discarded result holds an unhandled exception of unknown type";
  }
}

[[gnu::noinline, gnu::cold, noreturn]] inline void RethrowTaken(
    std::exception_ptr* slot) {
  std::exception_ptr ex = std::move(*slot);
  slot->~exception_ptr();
  std::rethrow_exception(std::move(ex));
}

struct Monostate {};

template <typename T>
class FutureState {
 public:
  using Stored = std::conditional_t<std::is_void_v<T>, Monostate, T>;

  FutureState() = default;
  ~FutureState() { DiscardAndReport(); }

  FutureState(const FutureState&) = delete;
  FutureState& operator=(const FutureState&) = delete;

  FutureState(FutureState&& other) noexcept { MoveFrom(std::move(other)); }

  FutureState& operator=(FutureState&& other) noexcept {
    if (this != &other) {
      DiscardAndReport();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  bool Available() const noexcept {
    return state_ == State::kValue || state_ == State::kException;
  }

  bool Failed() const noexcept { return state_ == State::kException; }

  template <typename... Args>
  void SetValue(Args&&... args) {
    DCHECK(state_ == State::kPending) << "FutureState satisfied twice";
    new (&storage_.value) Stored(std::forward<Args>(args)...);
    state_ = State::kValue;
  }

  void SetException(std::exception_ptr ex) {
    DCHECK(state_ == State::kPending) << "FutureState satisfied twice";
    DCHECK(ex != nullptr) << "null exception";
    new (&storage_.exception) std::exception_ptr(std::move(ex));
    state_ = State::kException;
  }

  T Take() {
    DCHECK(Available()) << "Take() on an unavailable FutureState";

    if (state_ == State::kException) {
      state_ = State::kEmpty;
      RethrowTaken(&storage_.exception);
    }

    [[maybe_unused]] Stored value = std::move(storage_.value);
    storage_.value.~Stored();
    state_ = State::kEmpty;
    if constexpr (!std::is_void_v<T>) {
      return value;
    }
  }

 private:
  enum class State : uint8_t {
    kEmpty,
    kPending,
    kValue,
    kException,
  };

  void DiscardAndReport() noexcept {
    if (state_ == State::kValue) {
      if constexpr (std::is_same_v<Stored, Status>) {
        if (!storage_.value.ok()) {
          ReportLostStatus(storage_.value);
        }
      }
      storage_.value.~Stored();
      state_ = State::kEmpty;
    } else if (state_ == State::kException) {
      ReportLostException(std::move(storage_.exception));
    }
  }

  void MoveFrom(FutureState&& other) noexcept {
    if (other.state_ == State::kValue) {
      new (&storage_.value) Stored(std::move(other.storage_.value));
      other.storage_.value.~Stored();
    } else if (other.state_ == State::kException) {
      new (&storage_.exception)
          std::exception_ptr(std::move(other.storage_.exception));
      other.storage_.exception.~exception_ptr();
    }
    state_ = std::exchange(other.state_, State::kEmpty);
  }

  union Storage {
    Storage() {}
    ~Storage() {}

    Stored value;
    std::exception_ptr exception;
  } storage_;

  State state_ = State::kPending;
};

template <typename T>
class Promise {
 public:
  Promise() noexcept : state_(&local_state_) {}
  ~Promise() { Abandon(); }

  Promise(Promise&& other) noexcept { MoveFrom(std::move(other)); }

  Promise& operator=(Promise&& other) noexcept {
    if (this != &other) {
      Abandon();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  Promise(const Promise&) = delete;
  Promise& operator=(const Promise&) = delete;

  Future<T> GetFuture();

  template <typename... Args>
  void SetValue(Args&&... args) {
    if (state_ == nullptr) {
      FutureState<T> discarded;
      discarded.SetValue(std::forward<Args>(args)...);
      return;
    }
    state_->SetValue(std::forward<Args>(args)...);
    ScheduleWaiter();
  }

  void SetException(std::exception_ptr ex) {
    if (state_ == nullptr) {
      ReportLostException(std::move(ex));  // never returns
    }
    state_->SetException(std::move(ex));
    ScheduleWaiter();
  }

  void SetFrom(FutureState<T>&& state) noexcept {
    if (state_ == nullptr) {
      return;
    }
    *state_ = std::move(state);
    ScheduleWaiter();
  }

 private:
  friend class Future<T>;
  friend class FutureAwaiter<T>;

  void SetWaiter(Task* waiter) noexcept {
    DCHECK(waiter_ == nullptr) << "Future already has a waiting task";
    waiter_ = waiter;
  }

  void ScheduleWaiter() noexcept {
    if (waiter_ != nullptr) {
      Schedule(std::exchange(waiter_, nullptr));
    }
  }

  void Abandon() noexcept {
    if (future_ == nullptr) {
      return;
    }

    DCHECK(state_ != nullptr);
    if (!state_->Available()) {
      state_->SetException(MakeBrokenPromise());
      ScheduleWaiter();
    }

    future_->promise_ = nullptr;
    future_ = nullptr;
    state_ = nullptr;
    waiter_ = nullptr;
  }

  void MoveFrom(Promise&& other) noexcept {
    future_ = other.future_;
    waiter_ = other.waiter_;

    if (other.state_ == &other.local_state_) {
      local_state_ = std::move(other.local_state_);
      state_ = &local_state_;
    } else {
      state_ = other.state_;
    }

    if (future_ != nullptr) {
      future_->promise_ = this;
    }

    other.future_ = nullptr;
    other.state_ = nullptr;
    other.waiter_ = nullptr;
  }

  FutureState<T> local_state_;
  FutureState<T>* state_ = nullptr;
  Future<T>* future_ = nullptr;
  Task* waiter_ = nullptr;
};

template <typename T>
class [[nodiscard]] Future {
 public:
  using value_type = T;
  using promise_type = CoroutinePromise<T>;

  ~Future() { Unlink(); }

  Future(Future&& other) noexcept
      : state_(std::move(other.state_)),
        promise_(std::exchange(other.promise_, nullptr)) {
    if (promise_ != nullptr) {
      promise_->future_ = this;
      promise_->state_ = &state_;
    }
  }

  Future& operator=(Future&& other) noexcept {
    if (this != &other) {
      Unlink();
      state_ = std::move(other.state_);
      promise_ = std::exchange(other.promise_, nullptr);
      if (promise_ != nullptr) {
        promise_->future_ = this;
        promise_->state_ = &state_;
      }
    }
    return *this;
  }

  Future(const Future&) = delete;
  Future& operator=(const Future&) = delete;

  bool Available() const noexcept { return state_.Available(); }
  bool Failed() const noexcept { return state_.Failed(); }

  T Get() {
    DCHECK(Available()) << "Get() on an unavailable Future";
    Unlink();
    return state_.Take();
  }

  FutureAwaiter<T> operator co_await() && noexcept;

 private:
  friend class Promise<T>;
  friend class FutureAwaiter<T>;
  template <typename U, typename... Args>
  friend Future<U> MakeReadyFuture(Args&&...);
  template <typename U>
  friend Future<U> MakeExceptionFuture(std::exception_ptr);

  Future() = default;

  explicit Future(Promise<T>* promise)
      : state_(std::move(promise->local_state_)), promise_(promise) {
    promise_->future_ = this;
    promise_->state_ = &state_;
  }

  void Unlink() noexcept {
    if (promise_ == nullptr) {
      return;
    }

    DCHECK(promise_->waiter_ == nullptr) << "unlinking an awaited Future";
    promise_->future_ = nullptr;
    promise_->state_ = nullptr;
    promise_->waiter_ = nullptr;
    promise_ = nullptr;
  }

  FutureState<T> state_;
  Promise<T>* promise_ = nullptr;
};

template <typename T>
Future<T> Promise<T>::GetFuture() {
  DCHECK(future_ == nullptr) << "GetFuture() called twice";
  DCHECK(state_ == &local_state_) << "GetFuture() called twice";
  DCHECK(waiter_ == nullptr);
  return Future<T>(this);
}

template <typename T>
struct FutureTraits {
  using Value = T;
  static constexpr bool kIsFuture = false;
};

template <typename T>
struct FutureTraits<Future<T>> {
  using Value = T;
  static constexpr bool kIsFuture = true;
};

template <typename Derived, typename T>
class CoroutinePromiseBase : public Task {
 public:
  Future<T> get_return_object() { return promise_.GetFuture(); }
  std::suspend_never initial_suspend() noexcept { return {}; }
  std::suspend_never final_suspend() noexcept { return {}; }

  void unhandled_exception() noexcept {
    promise_.SetException(std::current_exception());
  }

  void RunAndDispose() noexcept override {
    std::coroutine_handle<Derived>::from_promise(*static_cast<Derived*>(this))
        .resume();
  }

 protected:
  ~CoroutinePromiseBase() = default;

  Promise<T> promise_;

 private:
  friend Derived;
  CoroutinePromiseBase() = default;
};

template <typename T>
class CoroutinePromise final
    : public CoroutinePromiseBase<CoroutinePromise<T>, T> {
 public:
  template <typename U>
  void return_value(U&& value) {
    this->promise_.SetValue(std::forward<U>(value));
  }
};

template <>
class CoroutinePromise<void> final
    : public CoroutinePromiseBase<CoroutinePromise<void>, void> {
 public:
  void return_void() { this->promise_.SetValue(); }
};

template <typename T>
class FutureAwaiter {
 public:
  explicit FutureAwaiter(Future<T>&& future) noexcept
      : future_(std::move(future)) {}

  bool await_ready() const noexcept {
    return future_.Available() && !NeedPreempt();
  }

  template <TaskPromise P>
  void await_suspend(std::coroutine_handle<P> handle) noexcept {
    Task* task = &handle.promise();
    if (!future_.Available()) {
      DCHECK(future_.promise_ != nullptr)
          << "awaiting a Future without a Promise";
      future_.promise_->SetWaiter(task);
    } else {
      Schedule(task);
    }
  }

  T await_resume() { return future_.Get(); }

 private:
  Future<T> future_;
};

class YieldAwaiter {
 public:
  bool await_ready() const noexcept { return false; }

  template <TaskPromise P>
  void await_suspend(std::coroutine_handle<P> handle) noexcept {
    Schedule(&handle.promise());
  }

  void await_resume() const noexcept {}
};

class SleepAwaiter {
 public:
  explicit SleepAwaiter(uint64_t delay_ns) noexcept : delay_ns_(delay_ns) {}

  bool await_ready() const noexcept { return false; }

  template <TaskPromise P>
  void await_suspend(std::coroutine_handle<P> handle) {
    Task* task = &handle.promise();
    if (delay_ns_ == 0) {
      Schedule(task);
      return;
    }
    timer_.SetCallback([task] { Schedule(task); });
    timer_.ArmAfterNs(delay_ns_);
  }

  void await_resume() const noexcept {}

 private:
  Timer timer_;
  uint64_t delay_ns_;
};

template <typename T>
FutureAwaiter<T> Future<T>::operator co_await() && noexcept {
  return FutureAwaiter<T>(std::move(*this));
}

inline YieldAwaiter Yield() { return {}; }

inline SleepAwaiter Sleep(std::chrono::steady_clock::duration delay) {
  const auto ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(delay).count();
  return SleepAwaiter(ns > 0 ? static_cast<uint64_t>(ns) : 0);
}

template <typename T, typename... Args>
Future<T> MakeReadyFuture(Args&&... args) {
  Future<T> future;
  future.state_.SetValue(std::forward<Args>(args)...);
  return future;
}

template <typename T>
Future<T> MakeExceptionFuture(std::exception_ptr ex) {
  Future<T> future;
  future.state_.SetException(std::move(ex));
  return future;
}

inline Future<> WhenAll(std::vector<Future<>> futures) {
  std::exception_ptr first_exception;
  for (auto& future : futures) {
    try {
      co_await std::move(future);
    } catch (...) {
      if (!first_exception) {
        first_exception = std::current_exception();
      }
    }
  }
  if (first_exception) {
    std::rethrow_exception(first_exception);
  }
}

template <typename... Futures>
  requires(std::same_as<Futures, Future<>> && ...)
Future<> WhenAll(Futures... futures) {
  std::vector<Future<>> pending;
  pending.reserve(sizeof...(Futures));
  (pending.push_back(std::move(futures)), ...);
  return WhenAll(std::move(pending));
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_REACTOR_COROUTINE_H_
