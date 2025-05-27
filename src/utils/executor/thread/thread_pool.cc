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

#include <cassert>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include "glog/logging.h"

namespace dingofs {

class ThreadPool::Impl {
 public:
  Impl(int thread_num) : thread_num_(thread_num) {}

  ~Impl() { Stop(); }

  void Start() {
    std::unique_lock<std::mutex> lg(mutex_);
    threads_.resize(thread_num_);
    for (size_t i = 0; i < thread_num_; i++) {
      threads_[i] = std::thread([this, i] { ThreadProc(i); });
    }
  }

  void Stop() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      assert(!exit_);
      exit_ = true;
      condition_.notify_all();
    }

    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  int GetBackgroundThreads() {
    std::lock_guard<std::mutex> lock(mutex_);
    return thread_num_;
  }

  int GetTaskNum() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
  }

  void Execute(const std::function<void()>& task) {
    auto cp(task);
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(std::move(cp));
    condition_.notify_one();
  }

  void Execute(std::function<void()>&& task) {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(std::move(task));
    condition_.notify_one();
  }

 private:
  void ThreadProc(size_t thread_id) {
    VLOG(12) << "Thread " << thread_id << " started.";

    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);

        condition_.wait(lock, [this] { return exit_ || !tasks_.empty(); });

        if (exit_ && tasks_.empty()) {
          break;
        }

        task = std::move(tasks_.front());
        tasks_.pop();
      }

      (task)();
    }

    VLOG(12) << "Thread " << thread_id << " exit.";
  }

  mutable std::mutex mutex_;
  int thread_num_{0};
  bool exit_{false};
  std::condition_variable condition_;
  std::vector<std::thread> threads_;
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