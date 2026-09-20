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

#ifndef DINGOFS_UTILS_TIMER_TIMER_IMPL_H_
#define DINGOFS_UTILS_TIMER_TIMER_IMPL_H_

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "utils/executor/thread_pool.h"
#include "utils/executor/timer/timer.h"

namespace dingofs {

// Simple priority-queue timer on a dedicated thread. Producers touch only the
// mutex and heap; the timer thread sleeps when idle, so it does not contend
// for producer cache lines. Only notifies when the earliest deadline changes.
class TimerImpl final : public Timer {
 public:
  // caller owns the thread pool
  TimerImpl(ThreadPool* thread_pool);

  ~TimerImpl() override;

  bool Start() override;
  bool Stop() override;

  // Returns true only when the timer accepts ownership of func. The timer does
  // not retain a rejected function.
  bool Add(std::function<void()> func, int delay_ms) override;

  bool IsStopped() override;

 private:
  struct FunctionInfo {
    std::function<void()> fn;
    uint64_t next_run_time_us;

    explicit FunctionInfo(std::function<void()> p_fn,
                          uint64_t p_next_run_time_us)
        : fn(std::move(p_fn)), next_run_time_us(p_next_run_time_us) {}
  };

  struct RunTimeOrder {
    bool operator()(const FunctionInfo& f1, const FunctionInfo& f2) {
      return f1.next_run_time_us > f2.next_run_time_us;
    }
  };

  void Run();

  ThreadPool* thread_pool_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::unique_ptr<std::thread> thread_{nullptr};
  std::priority_queue<FunctionInfo, std::vector<FunctionInfo>, RunTimeOrder>
      heap_;
  bool running_{false};
};

}  // namespace dingofs

#endif  // DINGOFS_UTILS_TIMER_TIMER_IMPL_H_
