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

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <memory>
#include <string>

#include "utils/executor/thread_pool.h"

namespace dingofs {

// Fixed-size thread pool backed by folly CPUThreadPoolExecutor. Execute
// enqueues immediately; Stop drains all accepted work via join().
class ThreadPoolImpl final : public ThreadPool {
 public:
  ThreadPoolImpl(const std::string& name, int num_threads);

  ~ThreadPoolImpl() override;

  void Start() override;
  void Stop() override;

  int GetBackgroundThreads() override;
  int GetTaskNum() const override;

  void Execute(const std::function<void()>& task) override;
  void Execute(std::function<void()>&& task) override;

 private:
  const std::string name_;
  const int thread_num_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> pool_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_THREAD_POOL_IMPL_H_
