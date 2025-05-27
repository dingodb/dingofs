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

#include "utils/executor/executor_impl.h"

#include <glog/logging.h>

#include "utils/executor/timer_impl.h"

namespace dingofs {

ExecutorImpl::ExecutorImpl()
    : timer_(nullptr), pool_(nullptr), running_(false) {}

ExecutorImpl::~ExecutorImpl() {
  Stop();
  timer_.reset();
  pool_.reset();
}

bool ExecutorImpl::Start(int thread_num) {
  pool_.reset(NewThreadPool(thread_num));
  pool_->Start();
  timer_ = std::make_unique<TimerImpl>(pool_.get());
  CHECK(timer_->Start());
  running_.store(true);
  return true;
}

bool ExecutorImpl::Stop() {
  if (running_.load()) {
    CHECK(timer_->Stop());
    pool_->Stop();
    running_ = false;
    return true;
  } else {
    return false;
  }
}

bool ExecutorImpl::Execute(std::function<void()> func) {
  CHECK(running_);
  pool_->Execute(std::move(func));
  return true;
}

bool ExecutorImpl::Schedule(std::function<void()> func, int delay_ms) {
  CHECK(running_);
  timer_->Add(std::move(func), delay_ms);
  return true;
}

}  // namespace dingofs