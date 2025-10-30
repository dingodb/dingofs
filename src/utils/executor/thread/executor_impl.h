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

#ifndef DINGOFS_UITLS_THREAD_EXECUTOR_IMPL_H_
#define DINGOFS_UITLS_THREAD_EXECUTOR_IMPL_H_

#include <gflags/gflags_declare.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "utils/executor/executor.h"
#include "utils/executor/thread_pool.h"
#include "utils/executor/timer/timer.h"

namespace dingofs {

DECLARE_int32(executor_impl_bg_thread_num);

class ExecutorImpl final : public Executor {
 public:
  ExecutorImpl(const std::string& name)
      : ExecutorImpl(name, FLAGS_executor_impl_bg_thread_num) {}

  ExecutorImpl(const std::string& name, int thread_num)
      : name_(name),
        thread_num_(thread_num),
        timer_(nullptr),
        pool_(nullptr),
        running_(false) {}

  ~ExecutorImpl() override { Stop(); }

  bool Start() override;

  bool Stop() override;

  bool Execute(std::function<void()> func) override;

  bool Schedule(std::function<void()> func, int delay_ms) override;

  int ThreadNum() const override { return pool_->GetBackgroundThreads(); }

  int TaskNum() const override { return pool_->GetTaskNum(); }

  std::string Name() const override { return InternalName(); }

  static std::string InternalName() { return "ExecutorImpl"; }

 private:
  const std::string name_;
  const int thread_num_;
  std::unique_ptr<Timer> timer_;
  std::unique_ptr<ThreadPool> pool_;
  std::atomic_bool running_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_THREAD_EXECUTOR_IMPL_H_