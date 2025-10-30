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

#ifndef DINGOFS_UITLS_THREAD_POOL_IMPL_H_
#define DINGOFS_UITLS_THREAD_POOL_IMPL_H_

#include <butil/compiler_specific.h>

#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "utils/executor/thread_pool.h"

namespace dingofs {

class ThreadPoolImpl : public ThreadPool {
 public:
  ThreadPoolImpl(const std::string& name, int num_threads)
      : name_(name), thread_num_(num_threads) {}

  ~ThreadPoolImpl() override { Stop(); }

  void Start() override;

  void Stop() override;

  int GetBackgroundThreads() override;

  // Get the number of task scheduled in the ThreadPoolImpl
  int GetTaskNum() const override;

  // Submit a fire and forget jobs
  // This allows to submit the same job multiple times
  void Execute(const std::function<void()>&) override;

  // This moves the function in for efficiency
  void Execute(std::function<void()>&&) override;

 private:
  void ThreadProc(size_t thread_id);

  mutable std::mutex mutex_;
  const std::string name_;
  int thread_num_{0};
  bool running_{false};
  std::condition_variable condition_;
  std::vector<std::thread> threads_;
  std::queue<std::function<void()>> tasks_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_THREAD_POOL_IMPL_H_