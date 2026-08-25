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

#include "blockcache/core/reactor/timer.h"

#include <glog/logging.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <bit>
#include <cerrno>
#include <cstring>

#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

void Timer::ArmAtNs(uint64_t abs_ns) {
  CHECK(!queued_) << "timer already armed; Cancel() first";
  armed_ = true;
  queued_ = true;
  period_ns_ = 0;
  timeout_ns_ = abs_ns;
  ThisTimerService().Add(this);
}

void Timer::ArmAfterNs(uint64_t delay_ns) { ArmAtNs(TimestampNs() + delay_ns); }

void Timer::ArmPeriodic(std::chrono::steady_clock::duration period) {
  CHECK(!queued_) << "timer already armed; Cancel() first";
  uint64_t period_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(period).count());
  armed_ = true;
  queued_ = true;
  period_ns_ = period_ns;
  timeout_ns_ = TimestampNs() + period_ns;
  ThisTimerService().Add(this);
}

bool Timer::Cancel() {
  if (!armed_ && !queued_) {
    return false;
  }

  bool was_armed = armed_;
  armed_ = false;
  period_ns_ = 0;
  if (queued_) {
    ThisTimerService().Remove(this);
    queued_ = false;
  }
  return was_armed;
}

void TimerList::PushBack(Timer* t) {
  t->prev_ = tail_;
  t->next_ = nullptr;
  if (tail_ != nullptr) {
    tail_->next_ = t;
  } else {
    head_ = t;
  }
  tail_ = t;
}

Timer* TimerList::PopFront() {
  Timer* t = head_;
  if (t != nullptr) {
    Remove(t);
  }
  return t;
}

void TimerList::Remove(Timer* t) {
  if (t->prev_ != nullptr) {
    t->prev_->next_ = t->next_;
  } else {
    head_ = t->next_;
  }
  if (t->next_ != nullptr) {
    t->next_->prev_ = t->prev_;
  } else {
    tail_ = t->prev_;
  }
  t->prev_ = t->next_ = nullptr;
}

void TimerList::SpliceAllInto(TimerList* dst) {
  if (empty()) {
    return;
  }

  if (dst->tail_ != nullptr) {
    dst->tail_->next_ = head_;
    head_->prev_ = dst->tail_;
    dst->tail_ = tail_;
  } else {
    dst->head_ = head_;
    dst->tail_ = tail_;
  }
  head_ = tail_ = nullptr;
}

bool TimerSet::Insert(Timer* t) {
  uint64_t ts = t->timeout_ns_;
  int index = IndexOf(ts);
  buckets_[index].PushBack(t);
  MarkNonEmpty(index);
  if (ts < next_) {
    next_ = ts;
    return true;
  }
  return false;
}

void TimerSet::Remove(Timer* t) {
  int index = IndexOf(t->timeout_ns_);
  TimerList& list = buckets_[index];
  list.Remove(t);
  if (list.empty()) {
    MarkEmpty(index);
  }
}

TimerList TimerSet::Expire(uint64_t now) {
  TimerList expired;
  int index = IndexOf(now);

  if (index < kPastBucket) {
    uint64_t mask = (index + 1 < 64) ? (bits_ & (~0ull << (index + 1))) : 0;
    while (mask != 0) {
      int i = std::countr_zero(mask);
      mask &= mask - 1;
      buckets_[i].SpliceAllInto(&expired);
      bits_ &= ~(1ull << i);
    }
    if (past_nonempty_) {
      buckets_[kPastBucket].SpliceAllInto(&expired);
      past_nonempty_ = false;
    }
  }

  last_ = now;
  next_ = UINT64_MAX;

  TimerList& list = buckets_[index];
  if (index == kPastBucket) {
    past_nonempty_ = false;
  } else {
    bits_ &= ~(1ull << index);
  }

  TimerList pending;
  list.SpliceAllInto(&pending);
  while (!pending.empty()) {
    Timer* t = pending.PopFront();
    if (t->timeout_ns_ <= now) {
      expired.PushBack(t);
    } else {
      Insert(t);  // re-buckets finer relative to the new last_
    }
  }

  if (next_ == UINT64_MAX && !empty()) {
    const TimerList& finest = past_nonempty_
                                  ? buckets_[kPastBucket]
                                  : buckets_[63 - std::countl_zero(bits_)];
    for (Timer* t = finest.Front(); t != nullptr; t = t->next_) {
      next_ = next_ < t->timeout_ns_ ? next_ : t->timeout_ns_;
    }
  }
  return expired;
}

int TimerSet::IndexOf(uint64_t ts) const {
  if (ts <= last_) {
    return kPastBucket;
  }
  return std::countl_zero(ts ^ last_);  // 0..63
}

void TimerSet::MarkNonEmpty(int i) {
  if (i == kPastBucket) {
    past_nonempty_ = true;
  } else {
    bits_ |= 1ull << i;
  }
}

void TimerSet::MarkEmpty(int i) {
  if (i == kPastBucket) {
    past_nonempty_ = false;
  } else {
    bits_ &= ~(1ull << i);
  }
}

bool TimerQueue::Add(Timer* t) { return set_.Insert(t); }

void TimerQueue::Remove(Timer* t) {
  if (t->expired_) {
    expired_.Remove(t);
    t->expired_ = false;
  } else {
    set_.Remove(t);
  }
}

unsigned TimerQueue::RunExpired(uint64_t now) {
  expired_ = set_.Expire(now);
  for (Timer* t = expired_.Front(); t != nullptr; t = t->next_) {
    t->expired_ = true;
  }

  unsigned fired = 0;
  while (!expired_.empty()) {
    fired += Fire(expired_.PopFront()) ? 1 : 0;
  }
  return fired;
}

bool TimerQueue::Fire(Timer* t) {
  t->expired_ = false;
  t->queued_ = false;
  if (!t->armed_) {
    return false;  // cancelled while queued
  }

  t->armed_ = false;
  if (t->period_ns_ != 0) {
    RearmPeriodic(t);
  }

  RunCallback(t);
  return true;
}

void TimerQueue::RearmPeriodic(Timer* t) {
  t->timeout_ns_ = TimestampNs() + t->period_ns_;
  t->armed_ = true;
  t->queued_ = true;
  set_.Insert(t);
}

void TimerQueue::RunCallback(Timer* t) {
  try {
    t->cb_();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Fail to run timer callback: " << e.what();
  } catch (...) {
    LOG(ERROR) << "Fail to run timer callback: unknown exception";
  }
}

TimerService::TimerService(Dispatcher& dispatcher) : dispatcher_(&dispatcher) {
  timerfd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
  CHECK(timerfd_ >= 0) << "Fail to create timerfd: " << std::strerror(errno);
  dispatcher_->AddEvent(timerfd_, this, EventMode::kReadRepeat);
  CHECK(tls_timer_service == nullptr) << "one timer service per shard";
  tls_timer_service = this;
}

TimerService::~TimerService() {
  dispatcher_->DeleteEventAndWait(
      this);  // no OnCancelled(): we are what goes away
  tls_timer_service = nullptr;
  ::close(timerfd_);
}

void TimerService::OnReady() noexcept {
  queue_.RunExpired(TimestampNs());
  if (!queue_.empty()) {
    ArmTimerfd(queue_.NextTimeout());
  }
}

void TimerService::ArmTimerfd(uint64_t abs_ns) const {
  itimerspec its{};
  its.it_value.tv_sec = static_cast<time_t>(abs_ns / kNsPerSec);
  its.it_value.tv_nsec = static_cast<long>(abs_ns % kNsPerSec);
  if (::timerfd_settime(timerfd_, TFD_TIMER_ABSTIME, &its, nullptr) < 0) {
    PLOG(FATAL) << "Fail to arm timerfd";
  }
}

}  // namespace blockcache
}  // namespace dingofs
