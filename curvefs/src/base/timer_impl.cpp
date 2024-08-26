
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

#include "curvefs/src/base/timer_impl.h"

#include <sys/stat.h>

#include <mutex>

#include "bthread/bthread.h"
#include "bthread/types.h"
#include "glog/logging.h"

DEFINE_int32(timer_bg_bthread_num, 4, "background bthread number for timer");

namespace curvefs {
namespace base {

using namespace std::chrono;

class TimerImpl::BThreadPool {
 public:
  BThreadPool(int bthread_num) : bthread_num_(bthread_num) {
    bthread_mutex_init(&mutex_, nullptr);
    bthread_cond_init(&cond_, nullptr);
  }

  ~BThreadPool() {
    Stop();
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
  }

  void Start() {
    std::unique_lock<bthread_mutex_t> lg(mutex_);
    if (running_) {
      return;
    }

    running_ = true;

    threads_.resize(bthread_num_);
    for (int i = 0; i < bthread_num_; i++) {
      if (bthread_start_background(&threads_[i], nullptr,
                                   &TimerImpl::BThreadPool::BThreadRun,
                                   this) != 0) {
        LOG(FATAL) << "Fail to create bthread";
      }
    }
  }

  void Stop() {
    {
      std::unique_lock<bthread_mutex_t> lg(mutex_);
      if (!running_) {
        return;
      }

      running_ = false;
      bthread_cond_broadcast(&cond_);
    }

    for (auto& bthread : threads_) {
      bthread_join(bthread, nullptr);
    }
  }

  int GetBackgroundThreads() {
    std::unique_lock<bthread_mutex_t> lg(mutex_);
    return bthread_num_;
  }

  int GetTaskNum() {
    std::unique_lock<bthread_mutex_t> lg(mutex_);
    return tasks_.size();
  }

  void Execute(const std::function<void()>& task) {
    auto cp(task);
    std::lock_guard<bthread_mutex_t> lg(mutex_);
    tasks_.push(std::move(cp));
    bthread_cond_signal(&cond_);
  }

  void Execute(std::function<void()>&& task) {
    std::lock_guard<bthread_mutex_t> lg(mutex_);
    tasks_.push(std::move(task));
    bthread_cond_signal(&cond_);
  }

  static void* BThreadRun(void* arg) {
    auto* pool = reinterpret_cast<BThreadPool*>(arg);
    pool->ThreadProc(bthread_self());
    return nullptr;
  }

 private:
  void ThreadProc(bthread_t id) {
    VLOG(9) << "bthread id:" << id << " started.";

    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<bthread_mutex_t> lg(mutex_);

        if (!tasks_.empty()) {
          task = std::move(tasks_.front());
          tasks_.pop();
        } else {
          if (!running_) {
            // exit bthread
            break;
          } else {
            bthread_cond_wait(&cond_, &mutex_);
            continue;
          }
        }
      }

      CHECK(task);
      (task)();
    }

    VLOG(9) << "bthread id:" << id << " started.";
  }

  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  int bthread_num_{0};
  bool running_{false};

  std::vector<bthread_t> threads_;
  std::queue<std::function<void()>> tasks_;
};

TimerImpl::TimerImpl() : TimerImpl(FLAGS_timer_bg_bthread_num) {}

TimerImpl::TimerImpl(int bg_bthread_num)
    : thread_(nullptr), running_(false), bg_bthread_num_(bg_bthread_num) {
  thread_pool_ = std::make_unique<BThreadPool>(bg_bthread_num_);
}

TimerImpl::~TimerImpl() { Stop(); }

bool TimerImpl::Start() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (running_) {
    return false;
  }

  thread_pool_->Start();

  thread_ = std::make_unique<std::thread>(&TimerImpl::Run, this);
  running_ = true;

  return true;
}

bool TimerImpl::Stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!running_) {
      return false;
    }

    running_ = false;
    while (!heap_.empty()) {
      // TODO: add debug log
      heap_.pop();
    }

    cv_.notify_all();
  }

  if (thread_) {
    thread_->join();
  }

  thread_pool_->Stop();

  return true;
}

bool TimerImpl::IsStopped() {
  std::lock_guard<std::mutex> lk(mutex_);
  return !running_;
}

bool TimerImpl::Add(std::function<void()> func, int delay_ms) {
  CHECK(running_);
  auto now = steady_clock::now().time_since_epoch();
  uint64_t next =
      duration_cast<microseconds>(now + milliseconds(delay_ms)).count();

  FunctionInfo fn_info(std::move(func), next);
  std::lock_guard<std::mutex> lk(mutex_);
  heap_.push(std::move(fn_info));
  cv_.notify_all();
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
      thread_pool_->Execute(std::move(fn));
      heap_.pop();
    } else {
      cv_.wait_for(lk, microseconds(cur_fn.next_run_time_us - now));
    }
  }
}

}  // namespace base
}  // namespace curvefs