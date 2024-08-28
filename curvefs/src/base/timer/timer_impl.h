// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#ifndef CURVEFS_SRC_BASE_TIMER_TIMER_IMPL_H_
#define CURVEFS_SRC_BASE_TIMER_TIMER_IMPL_H_

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include "curvefs/src/base/timer/timer.h"
#include "gflags/gflags_declare.h"

DECLARE_int32(timer_bg_bthread_num);

namespace curvefs {
namespace base {
namespace timer {

class TimerImpl : public Timer {
 public:
  TimerImpl();

  TimerImpl(int bg_bthread_num);

  ~TimerImpl() override;

  bool Start() override;

  bool Stop() override;

  bool Add(std::function<void()> func, int delay_ms) override;

  bool IsStopped() override;

  int GetBgBthreadNum() const { return bg_bthread_num_; }

 private:
  void Run();

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

  std::mutex mutex_;
  std::condition_variable cv_;
  std::unique_ptr<std::thread> thread_;
  std::priority_queue<FunctionInfo, std::vector<FunctionInfo>, RunTimeOrder>
      heap_;
  bool running_;

  int bg_bthread_num_;
  class BThreadPool;
  std::unique_ptr<BThreadPool> thread_pool_;
};

}  // namespace timer
}  // namespace base
}  // namespace curvefs

#endif  // CURVEFS_SRC_BASE_TIMER_TIMER_IMPL_H_
