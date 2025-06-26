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

#include "utils/executor/bthread/bthread_pool.h"

#include <butil/compiler_specific.h>

#include <queue>

#include "bthread/bthread.h"
#include "bthread/types.h"

namespace dingofs {

void BThreadPool::BThreadProc(bthread_t id) {
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

static void* BThreadRun(void* arg) {
  auto* pool = reinterpret_cast<BThreadPool*>(arg);
  pool->BThreadProc(bthread_self());
  return nullptr;
}

void BThreadPool::Start() {
  std::unique_lock<bthread_mutex_t> lg(mutex_);
  if (running_) {
    return;
  }

  running_ = true;

  threads_.resize(bthread_num_);
  for (int i = 0; i < bthread_num_; i++) {
    if (bthread_start_background(&threads_[i], nullptr, &BThreadRun, this) !=
        0) {
      LOG(FATAL) << "Fail to create bthread";
    }
  }
}

void BThreadPool::Stop() {
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

int BThreadPool::GetBackgroundThreads() {
  std::unique_lock<bthread_mutex_t> lg(mutex_);
  return bthread_num_;
}

int BThreadPool::GetTaskNum() const {
  std::unique_lock<bthread_mutex_t> lg(mutex_);
  return tasks_.size();
}

void BThreadPool::Execute(const std::function<void()>& task) {
  auto cp(task);
  std::lock_guard<bthread_mutex_t> lg(mutex_);
  tasks_.push(std::move(cp));
  bthread_cond_signal(&cond_);
}

void BThreadPool::Execute(std::function<void()>&& task) {
  std::lock_guard<bthread_mutex_t> lg(mutex_);
  tasks_.push(std::move(task));
  bthread_cond_signal(&cond_);
}

}  // namespace dingofs