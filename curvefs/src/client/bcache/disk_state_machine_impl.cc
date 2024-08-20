// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "curvefs/src/client/bcache/disk_state_machine_impl.h"

#include <memory>
#include <mutex>
#include <shared_mutex>

#include "include/glog/logging.h"

namespace curvefs {
namespace client {

bool DiskStateMachineImpl::Start() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);

  if (running_) {
    return true;
  }

  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, EventThread, this) !=
      0) {
    LOG(ERROR) << "Fail start execution queue for process event";
    return false;
  }

  ticker_ = std::make_unique<std::thread>(&DiskStateMachineImpl::TickTock, this);

  running_ = true;

  return true;
}

bool DiskStateMachineImpl::Stop() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  if(!running_) {
    return true;
  }

  running_ = false;

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Fail stop execution queue for process event";
    return false;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Fail join execution queue for process event";
    return false;
  }

  return true;
}

void DiskStateMachineImpl::TickTock() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);

  while (running_) {
    state_->Tick();
  }
}

void DiskStateMachineImpl::OnEvent(DiskStateEvent event) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, event));
}

int DiskStateMachineImpl::EventThread(
    void* meta, bthread::TaskIterator<DiskStateEvent>& iter) {
  if (iter.is_queue_stopped()) {
    LOG(INFO) << "Execution queue is stopped";
    return 0;
  }

  auto* state_machine = reinterpret_cast<DiskStateMachineImpl*>(meta);

  for (; iter; ++iter) {
    state_machine->ProcessEvent(*iter);
  }

  return 0;
}

void DiskStateMachineImpl::ProcessEvent(DiskStateEvent event) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);

  LOG(WARNING) << "ProcessEvent: " << event << " in state "
               << state_->GetDiskState();

  switch (state_->GetDiskState()) {
    case DiskState::kDiskStateNormal:
      if (event == kDiskStateEventUnstable) {
        state_ = std::make_unique<UnstableDiskState>(this);
      }
    case kDiskStateUnStable:
      if (event == kDiskStateEventNormal) {
        state_ = std::make_unique<NormalDiskState>(this);
      } else if (event == kDiskStateEventDown) {
        state_ = std::make_unique<DownDiskState>(this);
      }
    case kDiskStateUnknown:
    case kDiskStateDown:
      break;
    default:
      LOG(FATAL) << "Unknown disk state " << state_->GetDiskState();
  }
}

}  // namespace client
}  // namespace curvefs