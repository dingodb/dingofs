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

#ifndef DINGOFS_UITLS_BTHREAD_BTHREAD_POOL_H_
#define DINGOFS_UITLS_BTHREAD_BTHREAD_POOL_H_

#include <butil/compiler_specific.h>

#include <queue>

#include "bthread/types.h"
#include "utils/executor/thread_pool.h"

namespace dingofs {

class BThreadPool : public ThreadPool {
 public:
  BThreadPool(int num_threads) : bthread_num_(num_threads) {}

  ~BThreadPool() override { Stop(); }

  void Start() override;

  void Stop() override;

  int GetBackgroundThreads() override;

  int GetTaskNum() const override;

  // Submit a fire and forget jobs
  // This allows to submit the same job multiple times
  void Execute(const std::function<void()>&) override;

  // This moves the function in for efficiency
  void Execute(std::function<void()>&&) override;

  void BThreadProc(size_t thread_id);

 private:
  mutable bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  int bthread_num_{0};
  bool running_{false};

  std::vector<bthread_t> threads_;
  std::queue<std::function<void()>> tasks_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_BTHREAD_BTHREAD_POOL_H_