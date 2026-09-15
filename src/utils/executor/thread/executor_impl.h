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

// Composes a folly-backed ThreadPoolImpl for immediate tasks with a
// folly-backed TimerImpl for delayed tasks. The owner serializes Start, Stop
// and destruction. Stop must not run on this executor's worker or timer
// thread. Tasks must not throw; violations terminate.
class ExecutorImpl final : public Executor {
 public:
  explicit ExecutorImpl(const std::string& name);
  ExecutorImpl(const std::string& name, int thread_num);
  ~ExecutorImpl() override;

  // thread_num must be positive. Returns false if already started.
  bool Start() override;

  // Closes admission, discards undispatched delayed tasks via Timer::Stop,
  // then drains accepted ready work via ThreadPool::Stop. Cancelled captures
  // are destroyed before returning. A completed Stop permits a subsequent
  // Start.
  bool Stop() override;

  // Must be called while started. Concurrent submissions are supported.
  bool Execute(std::function<void()> func) override;

  // May race Stop. Rejected tasks are not retained. Accepted tasks run on CPU
  // workers, no earlier than the deadline measured at entry, or are cancelled
  // by Stop. A nonpositive delay means ready asynchronously.
  bool Schedule(std::function<void()> func, int delay_ms) override;

  int ThreadNum() const override { return thread_num_; }
  int TaskNum() const override;
  std::string Name() const override { return InternalName(); }
  static std::string InternalName() { return "ExecutorImpl"; }

 private:
  const std::string name_;
  const int thread_num_;

  std::atomic<bool> running_{false};

  std::unique_ptr<ThreadPool> pool_;
  std::unique_ptr<Timer> timer_;
};

}  // namespace dingofs

#endif  // DINGOFS_UITLS_THREAD_EXECUTOR_IMPL_H_
