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

#include "utils/executor/thread/thread_pool_impl.h"

#include <cassert>

#include "glog/logging.h"

namespace dingofs {

void ThreadPoolImpl::ThreadProc(size_t thread_id) {
  VLOG(12) << "Thread " << thread_id << " started.";

  pthread_setname_np(pthread_self(), name_.substr(0, 15).c_str());

  while (true) {
    std::function<void()> task;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this] { return !tasks_.empty() || !running_; });

      if (!running_ && tasks_.empty()) {
        break;
      }

      if (!tasks_.empty()) {
        task = std::move(tasks_.front());
        tasks_.pop();
      }
    }  // end lock scope

    CHECK(task);
    (task)();
  }  // end of while loop

  VLOG(12) << "Thread " << thread_id << " exit.";
}

void ThreadPoolImpl::Start() {
  std::unique_lock<std::mutex> lg(mutex_);
  if (running_) {
    return;
  }

  running_ = true;

  threads_.resize(thread_num_);
  for (size_t i = 0; i < thread_num_; i++) {
    threads_[i] = std::thread([this, i] { ThreadProc(i); });
  }
}

void ThreadPoolImpl::Stop() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!running_) {
      return;
    }

    running_ = false;
    condition_.notify_all();
  }

  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

int ThreadPoolImpl::GetBackgroundThreads() {
  std::lock_guard<std::mutex> lock(mutex_);
  return thread_num_;
}

int ThreadPoolImpl::GetTaskNum() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return tasks_.size();
}

void ThreadPoolImpl::Execute(const std::function<void()>& task) {
  auto cp(task);
  std::lock_guard<std::mutex> lock(mutex_);
  tasks_.push(std::move(cp));
  condition_.notify_one();
}

void ThreadPoolImpl::Execute(std::function<void()>&& task) {
  std::lock_guard<std::mutex> lock(mutex_);
  tasks_.push(std::move(task));
  condition_.notify_one();
}

}  // namespace dingofs