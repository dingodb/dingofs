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

#ifndef DINGOFS_UITLS_BTHREAD_EXECUTOR_IMPL_H_
#define DINGOFS_UITLS_BTHREAD_EXECUTOR_IMPL_H_

#include <gflags/gflags_declare.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

#include "utils/executor/executor.h"
#include "utils/executor/thread_pool.h"
#include "utils/executor/timer/timer.h"

namespace dingofs {

DECLARE_int32(bthread_executor_bg_thread_num);

class BthreadExecutor final : public Executor {
 public:
  BthreadExecutor() : BthreadExecutor(FLAGS_bthread_executor_bg_thread_num) {}

  BthreadExecutor(int64_t bthread_num)
      : bthread_num_(bthread_num),
        timer_(nullptr),
        pool_(nullptr),
        running_(false) {}

  ~BthreadExecutor() override { Stop(); }

  bool Start() override;

  bool Stop() override;

  bool Execute(std::function<void()> func) override;

  bool Schedule(std::function<void()> func, int delay_ms) override;

  int ThreadNum() const override { return pool_->GetBackgroundThreads(); }

  int TaskNum() const override { return pool_->GetTaskNum(); }

  std::string Name() const override { return InternalName(); }

  static std::string InternalName() { return "BthreadExecutor"; }

 private:
  const int bthread_num_;
  std::unique_ptr<Timer> timer_;
  std::unique_ptr<ThreadPool> pool_;
  std::atomic_bool running_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_BTHREAD_EXECUTOR_IMPL_H_