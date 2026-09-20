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

#include "utils/executor/thread/executor_impl.h"

#include <glog/logging.h>

#include "utils/executor/thread/thread_pool_impl.h"
#include "utils/executor/timer/timer_impl.h"

namespace dingofs {

DEFINE_int32(executor_impl_bg_thread_num, 8,
             "background thread number for executor");

ExecutorImpl::ExecutorImpl(const std::string& name)
    : ExecutorImpl(name, FLAGS_executor_impl_bg_thread_num) {}

ExecutorImpl::ExecutorImpl(const std::string& name, int thread_num)
    : name_(name), thread_num_(thread_num) {}

ExecutorImpl::~ExecutorImpl() { Stop(); }

bool ExecutorImpl::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    return false;
  }
  CHECK_GT(thread_num_, 0);

  pool_ = std::make_unique<ThreadPoolImpl>(name_, thread_num_);
  pool_->Start();
  timer_ = std::make_unique<TimerImpl>(pool_.get());
  timer_->Start();
  running_.store(true, std::memory_order_release);
  return true;
}

bool ExecutorImpl::Stop() {
  // Close admission before waiting for existing submitters, so continuous
  // producers cannot starve the writer side of the admission lock.
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return false;
  }

  // Timer::Stop releases undispatched delayed-task captures; ThreadPool::Stop
  // joins workers after all timer dispatches have completed.
  timer_->Stop();
  timer_.reset();
  pool_->Stop();
  pool_.reset();
  return true;
}

bool ExecutorImpl::Execute(std::function<void()> func) {
  CHECK(running_.load(std::memory_order_relaxed));
  pool_->Execute(std::move(func));
  return true;
}

bool ExecutorImpl::Schedule(std::function<void()> func, int delay_ms) {
  if (!running_.load(std::memory_order_acquire)) {
    return false;
  }
  return timer_->Add(std::move(func), delay_ms);
}

int ExecutorImpl::TaskNum() const { return pool_ ? pool_->GetTaskNum() : 0; }

}  // namespace dingofs
