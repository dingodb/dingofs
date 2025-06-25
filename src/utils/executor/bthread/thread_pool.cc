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

#include "utils/executor/thread_pool.h"

#include <queue>

#include "bthread/bthread.h"
#include "bthread/types.h"

namespace dingofs {

class ThreadPool::Impl {
 public:
  Impl(int thread_num) : bthread_num_(thread_num) {
    bthread_mutex_init(&mutex_, nullptr);
    bthread_cond_init(&cond_, nullptr);
  }

  ~Impl() { Stop(); }

  static void* BThreadRun(void* arg) {
    auto* pool = reinterpret_cast<ThreadPool::Impl*>(arg);
    pool->ThreadProc(bthread_self());
    return nullptr;
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
                                   &ThreadPool::Impl::BThreadRun, this) != 0) {
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

 private:
  void ThreadProc(bthread_t id) {
    VLOG(12) << "bthread id:" << id << " started.";

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

    VLOG(12) << "bthread id:" << id << " exit";
  }

  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  int bthread_num_{0};
  bool running_{false};

  std::vector<bthread_t> threads_;
  std::queue<std::function<void()>> tasks_;
};

ThreadPool::ThreadPool(int num_threads) { impl_ = new Impl(num_threads); }

ThreadPool::~ThreadPool() { delete impl_; }

void ThreadPool::Start() { impl_->Start(); }

void ThreadPool::Stop() { impl_->Stop(); }

int ThreadPool::GetBackgroundThreads() { return impl_->GetBackgroundThreads(); }

int ThreadPool::GetTaskNum() const { return impl_->GetTaskNum(); }

void ThreadPool::Execute(const std::function<void()>& task) {
  impl_->Execute(task);
}

void ThreadPool::Execute(std::function<void()>&& task) {
  impl_->Execute(std::move(task));
}

ThreadPool* NewThreadPool(int num_threads) {
  ThreadPool* thread_pool = new ThreadPool(num_threads);
  return thread_pool;
}

}  // namespace dingofs