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

#ifndef DINGOFS_BLOCKCACHE_CORE_REACTOR_TIMER_H_
#define DINGOFS_BLOCKCACHE_CORE_REACTOR_TIMER_H_

#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "blockcache/core/reactor/dispatcher.h"
#include "blockcache/utils/time.h"
#include "utils/executor/timer/timer.h"

namespace dingofs {
namespace blockcache {

// Shard-thread only: idle --Arm()--> TimerSet --Expire--> expired list.
class Timer {
 public:
  using Callback = std::function<void()>;

  Timer() = default;
  explicit Timer(Callback cb) : cb_(std::move(cb)) {}
  ~Timer() { Cancel(); }

  Timer(const Timer&) = delete;
  Timer& operator=(const Timer&) = delete;

  void SetCallback(Callback cb) { cb_ = std::move(cb); }

  void ArmAtNs(uint64_t abs_ns);
  void ArmAfterNs(uint64_t delay_ns);
  void ArmPeriodic(std::chrono::steady_clock::duration period);
  bool Cancel();

 private:
  friend class TimerList;
  friend class TimerQueue;
  friend class TimerSet;

  Callback cb_;
  uint64_t timeout_ns_ = 0;  // absolute deadline, TimestampNs() units
  uint64_t period_ns_ = 0;   // != 0 -> periodic
  Timer* prev_ = nullptr;
  Timer* next_ = nullptr;
  bool armed_ = false;    // user intent: the callback should fire
  bool queued_ = false;   // linked in the active set or the expired list
  bool expired_ = false;  // sitting in the expired list mid-service
};

using TimerUPtr = std::unique_ptr<Timer>;

class TimerList {
 public:
  void PushBack(Timer* t);
  Timer* PopFront();
  void Remove(Timer* t);
  void SpliceAllInto(TimerList* dst);

  bool empty() const { return head_ == nullptr; }
  Timer* Front() const { return head_; }

 private:
  Timer* head_ = nullptr;
  Timer* tail_ = nullptr;
};

class TimerSet {
 public:
  bool Insert(Timer* t);
  void Remove(Timer* t);
  TimerList Expire(uint64_t now);
  uint64_t NextTimeout() const { return last_ > next_ ? last_ : next_; }
  bool empty() const { return bits_ == 0 && !past_nonempty_; }

 private:
  static constexpr int kPastBucket = 64;  // timeout <= last_

  int IndexOf(uint64_t ts) const;
  void MarkNonEmpty(int i);
  void MarkEmpty(int i);

  TimerList buckets_[kPastBucket + 1];
  uint64_t bits_ = 0;           // non-empty flags, buckets 0..63
  bool past_nonempty_ = false;  // bucket 64
  uint64_t last_ = 0;           // reference point: `now` of the last Expire
  uint64_t next_ = UINT64_MAX;  // lower bound of the earliest deadline
};

class TimerQueue {
 public:
  bool Add(Timer* t);
  void Remove(Timer* t);
  unsigned RunExpired(uint64_t now);
  bool empty() const { return set_.empty(); }
  uint64_t NextTimeout() const { return set_.NextTimeout(); }

 private:
  bool Fire(Timer* t);  // true if the callback ran
  void RearmPeriodic(Timer* t);
  static void RunCallback(Timer* t);

  TimerSet set_;
  TimerList expired_;  // non-empty only inside RunExpired()
};

class TimerService final : public Event {
 public:
  explicit TimerService(Dispatcher& dispatcher);
  ~TimerService();

  TimerService(const TimerService&) = delete;
  TimerService& operator=(const TimerService&) = delete;

  void Add(Timer* t) {
    if (queue_.Add(t)) {
      ArmTimerfd(queue_.NextTimeout());
    }
  }
  void Remove(Timer* t) { queue_.Remove(t); }

 private:
  void OnReady() noexcept override;
  void ArmTimerfd(uint64_t abs_ns) const;

  TimerQueue queue_;
  Dispatcher* dispatcher_;
  int timerfd_ = -1;
};

inline thread_local TimerService* tls_timer_service = nullptr;

inline TimerService& ThisTimerService() {
  DCHECK(tls_timer_service != nullptr) << "no timer service on this thread";
  return *tls_timer_service;
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_REACTOR_TIMER_H_
