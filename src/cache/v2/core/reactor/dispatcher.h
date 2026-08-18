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

#ifndef DINGOFS_CACHE_V2_CORE_REACTOR_DISPATCHER_H_
#define DINGOFS_CACHE_V2_CORE_REACTOR_DISPATCHER_H_

#include <glog/logging.h>
#include <liburing.h>

#include <chrono>
#include <cstdint>

#include "cache/v2/core/reactor/preempt.h"

namespace dingofs {
namespace cache {
namespace v2 {

enum class EventMode : uint8_t {
  kReadRepeat,
  kPollOnce,
};

class Event {
 public:
  virtual void OnReady() noexcept = 0;
  virtual void OnCancelled() noexcept {}

 protected:
  ~Event() { DCHECK(state_ == State::kIdle) << "Event destroyed while armed"; }

 private:
  friend class Dispatcher;
  friend class EventList;

  enum class State : uint8_t {
    kIdle,          // not registered
    kArmed,         // one submission in flight
    kPendingRearm,  // fired; the batch re-posts it on the way out
    kCancelling,    // a cancel is in flight; retires when it lands
  };

  uint64_t buffer_ = 0;
  Event* next_ = nullptr;
  int fd_ = -1;
  EventMode mode_ = EventMode::kReadRepeat;
  State state_ = State::kIdle;
  bool notify_ = false;
  bool quiet_ = false;
};

class EventList {
 public:
  void Push(Event* ev) {
    ev->next_ = head_;
    head_ = ev;
  }

  Event* PopFront() {
    Event* ev = head_;
    if (ev != nullptr) {
      head_ = ev->next_;
      ev->next_ = nullptr;
    }
    return ev;
  }

  EventList TakeAll() {
    EventList taken;
    taken.head_ = head_;
    head_ = nullptr;
    return taken;
  }

  bool empty() const { return head_ == nullptr; }

 private:
  Event* head_ = nullptr;
};

class EventRing {
 public:
  explicit EventRing(unsigned queue_len);
  ~EventRing() { Exit(); }

  EventRing(const EventRing&) = delete;
  EventRing& operator=(const EventRing&) = delete;

  void Exit();

  io_uring_sqe* TryGetSqe() { return io_uring_get_sqe(&ring_); }

  unsigned PeekBatch(io_uring_cqe** cqes, unsigned n) {
    return io_uring_peek_batch_cqe(&ring_, cqes, n);
  }

  void Advance(unsigned n) { io_uring_cq_advance(&ring_, n); }
  void Submit();
  void SubmitAndWait();

  bool HasReady() const { return io_uring_cq_ready(&ring_) > 0; }
  bool HasUnsubmitted() const { return io_uring_sq_ready(&ring_) != 0; }
  PreemptMonitor Monitor() const;

 private:
  void Init(unsigned queue_len);
  void CheckOpcodes();

  io_uring ring_;
  bool open_ = false;
};

class QuotaTicker final : public Event {
 public:
  explicit QuotaTicker(std::chrono::microseconds quota);
  ~QuotaTicker();

  QuotaTicker(const QuotaTicker&) = delete;
  QuotaTicker& operator=(const QuotaTicker&) = delete;

  void Arm() const;
  void Disarm() const;

  int fd() const { return timerfd_; }

 private:
  void OnReady() noexcept override {}  // landing the CQE is the whole job

  std::chrono::microseconds quota_;
  int timerfd_ = -1;
};

class Waker final : public Event {
 public:
  Waker();
  ~Waker();

  Waker(const Waker&) = delete;
  Waker& operator=(const Waker&) = delete;

  void Ring() const;

  int fd() const { return eventfd_; }

 private:
  void OnReady() noexcept override {}

  int eventfd_ = -1;
};

struct DispatcherOption {
  unsigned queue_len = 32;
  std::chrono::microseconds task_quota{500};
};

class Dispatcher {
 public:
  class RunScope {
   public:
    explicit RunScope(Dispatcher& dispatcher);
    ~RunScope();

    RunScope(const RunScope&) = delete;
    RunScope& operator=(const RunScope&) = delete;

   private:
    Dispatcher& dispatcher_;
  };

  explicit Dispatcher(const DispatcherOption& option = {});
  ~Dispatcher();

  Dispatcher(const Dispatcher&) = delete;
  Dispatcher& operator=(const Dispatcher&) = delete;

  void AddEvent(int fd, Event* ev, EventMode mode);
  bool DeleteEvent(Event* ev);
  void DeleteEventAndWait(Event* ev);
  unsigned ProcessEvents();
  void WaitForEvent();
  void Notify() const { waker_.Ring(); }

  bool HasReadyEvent() const { return ring_.HasReady(); }

 private:
  static constexpr unsigned kCqBatch = 256;

  void Arm(Event* ev);
  void Disarm(Event* ev);  // asks; the event retires when -ECANCELED lands
  bool Dispatch(Event* ev) noexcept;  // true if it counted as work
  void DrainRearm();
  void DrainCancelled();
  io_uring_sqe* GetSqe();
  static void ForceIdle(Event* ev) { ev->state_ = Event::State::kIdle; }

  EventRing ring_;
  QuotaTicker ticker_;
  Waker waker_;
  EventList rearm_;
  EventList cancelled_;
  bool dispatching_ = false;
};

inline thread_local Dispatcher* tls_dispatcher = nullptr;

inline Dispatcher& ThisDispatcher() {
  DCHECK(tls_dispatcher != nullptr) << "no dispatcher on this thread";
  return *tls_dispatcher;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_REACTOR_DISPATCHER_H_
