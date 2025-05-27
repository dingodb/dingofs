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

#ifndef DINGOFS_UITLS_EXECUTOR_IMPL_H_
#define DINGOFS_UITLS_EXECUTOR_IMPL_H_

#include <atomic>
#include <functional>
#include <memory>

#include "utils/executor/executor.h"
#include "utils/executor/thread_pool.h"
#include "utils/executor/timer.h"

namespace dingofs {

class ExecutorImpl final : public Executor {
 public:
  ExecutorImpl();

  ~ExecutorImpl() override;

  bool Start(int thread_num) override;

  bool Stop() override;

  bool Execute(std::function<void()> func) override;

  bool Schedule(std::function<void()> func, int delay_ms) override;

  int ThreadNum() const override { return pool_->GetBackgroundThreads(); }

  std::string Name() const override { return InternalName(); }

  static std::string InternalName() { return "ExecutorImpl"; }

 private:
  std::unique_ptr<Timer> timer_;
  std::unique_ptr<ThreadPool> pool_;
  std::atomic_bool running_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_EXECUTOR_IMPL_H_