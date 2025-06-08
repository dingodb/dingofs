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

#include "cache/utils/state_machine_impl.h"

#include <glog/logging.h>

#include <functional>
#include <memory>
#include <mutex>

#include "base/time/time.h"
#include "cache/config/config.h"
#include "cache/utils/state_machine.h"
#include "utils/executor/timer_impl.h"

namespace dingofs {
namespace cache {

// normal
void NormalState::IOErr() {
  io_error_count_.fetch_add(1);
  if (io_error_count_.load() > FLAGS_state_normal2unstable_io_error_num) {
    state_machine->OnEvent(StateEvent::kStateEventUnstable);
  }
}

void NormalState::Tick() { io_error_count_.store(0); }

// unstable
void UnstableState::IOSucc() {
  io_succ_count_.fetch_add(1);
  if (io_succ_count_.load() > FLAGS_state_unstable2normal_io_succ_num) {
    state_machine->OnEvent(StateEvent::kStateEventNormal);
  }
}

void UnstableState::Tick() {
  uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
  if (now - start_time_ > (uint64_t)FLAGS_state_unstable2down_s) {
    state_machine->OnEvent(StateEvent::kStateEventDown);
  }

  io_succ_count_.store(0);
}

StateMachineImpl::StateMachineImpl(OnStateChangeFunc on_stage_change_func)
    : running_(false),
      state_(std::make_unique<BaseState>(this)),
      on_stage_change_func_(on_stage_change_func) {}

bool StateMachineImpl::Start() {
  std::lock_guard<BthreadMutex> lk(mutex_);

  if (running_) {
    return true;
  }

  state_ = std::make_unique<NormalState>(this);
  on_stage_change_func_(state_->GetState());

  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;
  if (bthread::execution_queue_start(&disk_event_queue_id_, &options,
                                     EventThread, this) != 0) {
    LOG(ERROR) << "Fail start execution queue for process event.";
    return false;
  }

  timer_ = std::make_unique<TimerImpl>();
  CHECK(timer_->Start());

  running_ = true;
  timer_->Add([this] { TickTock(); }, FLAGS_state_tick_duration_s * 1000);

  LOG(INFO) << "Success start state machine.";
  return true;
}

bool StateMachineImpl::Stop() {
  std::lock_guard<BthreadMutex> lk(mutex_);

  if (!running_) {
    return true;
  }

  LOG(INFO) << "Try to stop state machine...";
  running_ = false;

  if (bthread::execution_queue_stop(disk_event_queue_id_) != 0) {
    LOG(ERROR) << "Fail stop execution queue for process event.";
    return false;
  } else if (bthread::execution_queue_join(disk_event_queue_id_) != 0) {
    LOG(ERROR) << "Fail join execution queue for process event.";
    return false;
  }

  timer_->Stop();
  on_stage_change_func_(kStateUnknown);

  return true;
}

void StateMachineImpl::TickTock() {
  std::lock_guard<BthreadMutex> lk(mutex_);
  if (!running_) {
    return;
  }

  state_->Tick();
  timer_->Add([this] { TickTock(); }, FLAGS_state_tick_duration_s * 1000);
}

void StateMachineImpl::OnEvent(StateEvent event) {
  CHECK_EQ(0, bthread::execution_queue_execute(disk_event_queue_id_, event));
}

int StateMachineImpl::EventThread(void* meta,
                                  bthread::TaskIterator<StateEvent>& iter) {
  if (iter.is_queue_stopped()) {
    LOG(INFO) << "Execution queue is stopped.";
    return 0;
  }

  auto* state_machine = reinterpret_cast<StateMachineImpl*>(meta);
  for (; iter; ++iter) {
    state_machine->ProcessEvent(*iter);
  }
  return 0;
}

void StateMachineImpl::ProcessEvent(StateEvent event) {
  std::lock_guard<BthreadMutex> lk(mutex_);

  LOG(INFO) << "ProcessEvent event: " << StateEventToString(event)
            << " in state:" << StateToString(state_->GetState());

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
      LOG(FATAL) << "Unknown disk state " << state_->GetState();
  }

  on_stage_change_func_(state_->GetState());

  LOG(INFO) << "After process, current disk state is "
            << StateToString(state_->GetState());
}

}  // namespace cache
}  // namespace dingofs
