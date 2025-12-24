// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "cache/iutil/state_machine_impl.h"

#include <brpc/reloadable_flags.h>
#include <bthread/mutex.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <functional>
#include <memory>
#include <mutex>

#include "cache/iutil/state_machine.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {
namespace iutil {

StateMachineImpl::StateMachineImpl(IConfiguration config)
    : running_(false),
      config_(config),
      state_(std::make_unique<BaseState>(this)),
      executor_(std::make_unique<BthreadExecutor>()) {}

bool StateMachineImpl::Start() {
  std::lock_guard<bthread::Mutex> lk(mutex_);

  if (running_) {
    return true;
  }

  LOG(INFO) << "StateMachineImpl is starting...";

  state_ = std::make_unique<NormalState>(this);

  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;
  if (bthread::execution_queue_start(&disk_event_queue_id_, &options,
                                     EventThread, this) != 0) {
    LOG(ERROR) << "Fail to start ExecutionQueue for process event";
    return false;
  }

  CHECK(executor_->Start());
  executor_->Schedule([this] { TickTock(); },
                      Config().tick_duration_s() * 1000);

  running_ = true;
  LOG(INFO) << "StateMachineImpl is up";
  return true;
}

bool StateMachineImpl::Shutdown() {
  std::lock_guard<bthread::Mutex> lk(mutex_);

  if (!running_.exchange(false)) {
    LOG(WARNING) << "StateMachineImpl is already down";
    return true;
  }

  LOG(INFO) << "StateMachineImpl is shutting down...";

  if (bthread::execution_queue_stop(disk_event_queue_id_) != 0) {
    LOG(ERROR) << "Fail to stop ExecutionQueue";
    return false;
  } else if (bthread::execution_queue_join(disk_event_queue_id_) != 0) {
    LOG(ERROR) << "Fail to join ExecutionQueue";
    return false;
  }

  executor_->Stop();

  LOG(INFO) << "StateMachineImpl is down";
  return true;
}

void StateMachineImpl::Success(int n) {
  if (running_) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    state_->Success(n);
  }
}

void StateMachineImpl::Error(int n) {
  if (running_) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    state_->Error(n);
  }
}

State StateMachineImpl::GetState() const {
  std::lock_guard<bthread::Mutex> lock(mutex_);
  return state_->GetState();
}

void StateMachineImpl::OnEvent(StateEvent event) {
  CHECK_EQ(0, bthread::execution_queue_execute(disk_event_queue_id_, event));
}

void StateMachineImpl::TickTock() {
  std::lock_guard<bthread::Mutex> lock(mutex_);
  if (!running_) {
    return;
  }

  state_->Tick();
  executor_->Schedule([this] { TickTock(); },
                      Config().tick_duration_s() * 1000);
}

int StateMachineImpl::EventThread(void* meta,
                                  bthread::TaskIterator<StateEvent>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = reinterpret_cast<StateMachineImpl*>(meta);
  for (; iter; ++iter) {
    self->ProcessEvent(*iter);
  }
  return 0;
}

void StateMachineImpl::ProcessEvent(StateEvent event) {
  std::lock_guard<bthread::Mutex> lock(mutex_);
  auto old_state = state_->GetState();
  switch (state_->GetState()) {
    case State::kStateNormal:
      if (event == kStateEventUnstable) {
        state_ = std::make_unique<UnstableState>(this);
      }
      break;
    case kStateUnStable:
      if (event == kStateEventNormal) {
        state_ = std::make_unique<NormalState>(this);
      } else if (event == kStateEventDown) {
        state_ = std::make_unique<DownState>(this);
      }
      break;
    case kStateUnknown:
    case kStateDown:
      break;
    default:
      LOG(FATAL) << "Unknown state=" << state_->GetState();
  }

  LOG(INFO) << "Successfully transfer state=" << StateToString(old_state)
            << " to state=" << StateToString(state_->GetState());
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
