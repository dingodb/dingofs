
// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils/executor/timer/timer_impl.h"

#include <glog/logging.h>

#include <chrono>
#include <utility>
#include <vector>

namespace dingofs {

using namespace std::chrono;

TimerImpl::TimerImpl(ThreadPool* thread_pool) : thread_pool_(thread_pool) {}

TimerImpl::~TimerImpl() { Stop(); }

bool TimerImpl::Start() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (running_) {
    return false;
  }

  thread_ = std::make_unique<std::thread>(&TimerImpl::Run, this);
  running_ = true;

  return true;
}

bool TimerImpl::Stop() {
  decltype(heap_) cancelled;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!running_) {
      return false;
    }

    running_ = false;
    heap_.swap(cancelled);
    cv_.notify_all();
  }

  if (thread_) {
    thread_->join();
  }

  // Destroy cancelled functions after releasing mutex_. Their captures may
  // release the last reference to objects with non-trivial destructors.
  return true;
}

bool TimerImpl::IsStopped() {
  std::lock_guard<std::mutex> lk(mutex_);
  return !running_;
}

bool TimerImpl::Add(std::function<void()> func, int delay_ms) {
  auto now = steady_clock::now().time_since_epoch();
  uint64_t next =
      duration_cast<microseconds>(now + milliseconds(delay_ms)).count();

  FunctionInfo fn_info(std::move(func), next);
  std::lock_guard<std::mutex> lk(mutex_);
  if (!running_) {
    LOG(WARNING) << "Fail add func, delay_ms:" << delay_ms
                 << ", timer is not running";
    return false;
  }

  heap_.push(std::move(fn_info));

  // Run only needs to reconsider its wait when the earliest deadline changes.
  // This avoids waking the timer thread (and contending its cache lines) for
  // tasks that do not affect the current minimum.
  const bool wake = heap_.size() == 1 || next < heap_.top().next_run_time_us;
  if (wake) {
    cv_.notify_one();
  }
  return true;
}

void TimerImpl::Run() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (running_) {
    if (heap_.empty()) {
      cv_.wait(lk);
      continue;
    }

    const auto& cur_fn = heap_.top();
    uint64_t now =
        duration_cast<microseconds>(steady_clock::now().time_since_epoch())
            .count();
    if (cur_fn.next_run_time_us <= now) {
      std::function<void()> fn = cur_fn.fn;
      heap_.pop();
      // Submit to the thread pool without holding mutex_: pool admission may
      // contend on its own queue, and holding the timer lock here would block
      // concurrent producers calling Add().
      lk.unlock();
      thread_pool_->Execute(std::move(fn));
      lk.lock();
    } else {
      cv_.wait_for(lk, microseconds(cur_fn.next_run_time_us - now));
    }
  }
}

}  // namespace dingofs
