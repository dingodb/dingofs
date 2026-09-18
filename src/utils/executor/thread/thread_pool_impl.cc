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

#include "utils/executor/thread/thread_pool_impl.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <utility>

namespace dingofs {

ThreadPoolImpl::ThreadPoolImpl(const std::string& name, int num_threads)
    : name_(name), thread_num_(num_threads) {}

ThreadPoolImpl::~ThreadPoolImpl() { Stop(); }

void ThreadPoolImpl::Start() {
  if (pool_) {
    return;
  }

  pool_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      std::make_pair(thread_num_, thread_num_),
      folly::CPUThreadPoolExecutor::makeLifoSemQueue(),
      std::make_shared<folly::NamedThreadFactory>(name_));
}

void ThreadPoolImpl::Stop() {
  if (!pool_) {
    return;
  }

  pool_->join();
  pool_.reset();
}

int ThreadPoolImpl::GetBackgroundThreads() { return thread_num_; }

int ThreadPoolImpl::GetTaskNum() const {
  return pool_ ? static_cast<int>(pool_->getTaskQueueSize()) : 0;
}

void ThreadPoolImpl::Execute(const std::function<void()>& task) {
  pool_->add([task]() { task(); });
}

void ThreadPoolImpl::Execute(std::function<void()>&& task) {
  pool_->add([task = std::move(task)]() mutable noexcept { task(); });
}

}  // namespace dingofs
