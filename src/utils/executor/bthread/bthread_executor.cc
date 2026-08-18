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

#include "utils/executor/bthread/bthread_executor.h"

#include <glog/logging.h>

#include <memory>

#include "utils/executor/bthread/bthread_pool.h"
#include "utils/executor/timer/timer_impl.h"

namespace dingofs {

DEFINE_int32(bthread_executor_bg_thread_num, 16,
             "background thread number for executor");

bool BthreadExecutor::Start() {
  pool_ = std::make_unique<BThreadPool>(bthread_num_);
  pool_->Start();
  timer_ = std::make_unique<TimerImpl>(pool_.get());
  CHECK(timer_->Start());
  running_.store(true, std::memory_order_release);
  return true;
}

bool BthreadExecutor::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return false;
  }

  CHECK(timer_->Stop());
  pool_->Stop();
  return true;
}

bool BthreadExecutor::Execute(std::function<void()> func) {
  CHECK(running_);
  pool_->Execute(std::move(func));
  return true;
}

bool BthreadExecutor::Schedule(std::function<void()> func, int delay_ms) {
  if (!running_.load(std::memory_order_acquire)) {
    return false;
  }
  return timer_->Add(std::move(func), delay_ms);
}

}  // namespace dingofs