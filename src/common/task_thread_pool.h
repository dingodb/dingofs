/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/*
 * Project: dingo
 * Created Date: 2023-02-14
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_COMMON_TASK_THREAD_POOL_H_
#define DINGOFS_SRC_COMMON_TASK_THREAD_POOL_H_

#include <condition_variable>
#include <mutex>
#include <utility>

#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace common {

template <typename MutexT = std::mutex,
          typename CondVarT = std::condition_variable>
class TaskThreadPool2 : public dingofs::utils::TaskThreadPool<MutexT, CondVarT> {
  using Base = dingofs::utils::TaskThreadPool<MutexT, CondVarT>;

 public:
  template <class F, class... Args>
  bool Enqueue(F&& f, Args&&... args) {
    std::unique_lock<MutexT> guard(Base::mutex_);

    if (!Base::running_.load(std::memory_order_acquire)) {
      // When stopped, running_ false recovery
      return false;
    }

    while (Base::IsFullUnlock()) {
      Base::notFull_.wait(guard);
    }
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    Base::queue_.push_back(std::move(task));
    Base::notEmpty_.notify_one();
    return true;
  }

  dingofs::utils::Task Take() {
    std::unique_lock<MutexT> guard(Base::mutex_);
    while (Base::queue_.empty() &&
           Base::running_.load(std::memory_order_acquire)) {
      Base::notEmpty_.wait(guard);
    }
    dingofs::utils::Task task;
    if (!Base::queue_.empty()) {
      task = std::move(Base::queue_.front());
      Base::queue_.pop_front();
      Base::notFull_.notify_one();
      std::unique_lock<MutexT> guard(executingMutex_);
      ++executing_;
    }
    return task;
  }

  virtual void ThreadFunc() {
    while (Base::running_.load(std::memory_order_acquire)) {
      dingofs::utils::Task task(Take());
      if (task) {
        task();
        std::unique_lock<MutexT> guard(executingMutex_);
        --executing_;
      }
    }
  }

  int QueueSize() const {
    std::lock_guard<MutexT> guard(Base::mutex_);
    std::lock_guard<MutexT> guardExecuting(executingMutex_);
    return Base::queue_.size() + executing_;
  }

 protected:
  int executing_ = 0;
  mutable MutexT executingMutex_;
};

}  // namespace common
}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_TASK_THREAD_POOL_H_
