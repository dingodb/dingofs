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

#include "cache/common/state_machine_impl.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <functional>
#include <memory>
#include <mutex>

#include "cache/common/macro.h"
#include "cache/common/state_machine.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

// Disk state
DEFINE_uint32(disk_state_tick_duration_s, 60,
              "duration in seconds for the disk state tick");
DEFINE_validator(disk_state_tick_duration_s, brpc::PassValidate);

DEFINE_uint32(disk_state_normal2unstable_error_num, 3,
              "number of errors to trigger unstable state from normal state");
DEFINE_validator(disk_state_normal2unstable_error_num, brpc::PassValidate);

DEFINE_uint32(disk_state_unstable2normal_succ_num, 10,
              "number of successes to trigger normal state from "
              "unstable state");
DEFINE_validator(disk_state_unstable2normal_succ_num, brpc::PassValidate);

DEFINE_uint32(disk_state_unstable2down_s, 1800,
              "duration in seconds to trigger down state from unstable state");
DEFINE_validator(disk_state_unstable2down_s, brpc::PassValidate);

// Node state
DEFINE_uint32(cache_node_state_tick_duration_s, 30,
              "duration in seconds for the cache node state tick");
DEFINE_validator(cache_node_state_tick_duration_s, brpc::PassValidate);

DEFINE_uint32(
    cache_node_state_normal2unstable_error_num, 10,
    "number of errors to trigger unstable cache state from normal state");
DEFINE_validator(cache_node_state_normal2unstable_error_num,
                 brpc::PassValidate);

DEFINE_uint32(cache_node_state_unstable2normal_succ_num, 3,
              "number of successes to trigger normal state from "
              "unstable state");
DEFINE_validator(cache_node_state_unstable2normal_succ_num, brpc::PassValidate);

DEFINE_uint32(cache_node_state_unstable2down_s, 604800,  // 7 days
              "duration in seconds to trigger down state from unstable state");
DEFINE_validator(cache_node_state_unstable2down_s, brpc::PassValidate);

uint32_t BaseState::CfgStateNormal2UnstableErrorNum() {
  if (state_machine->GetType() == kDiskStateMachine) {
    return FLAGS_disk_state_normal2unstable_error_num;
  }
  return FLAGS_cache_node_state_normal2unstable_error_num;
}

uint32_t BaseState::CfgStateUnstable2NormalSuccNum() {
  if (state_machine->GetType() == kDiskStateMachine) {
    return FLAGS_disk_state_unstable2normal_succ_num;
  }
  return FLAGS_cache_node_state_unstable2normal_succ_num;
}

uint32_t BaseState::CfgStateUnstable2downS() {
  if (state_machine->GetType() == kDiskStateMachine) {
    return FLAGS_disk_state_unstable2down_s;
  }
  return FLAGS_cache_node_state_unstable2down_s;
}

// normal
NormalState::NormalState(StateMachine* state_machine)
    : BaseState(state_machine) {}

void NormalState::Error() {
  error_count_.fetch_add(1);
  if (error_count_.load() > CfgStateNormal2UnstableErrorNum()) {
    state_machine->OnEvent(StateEvent::kStateEventUnstable);
  }
}

void NormalState::Tick() { error_count_.store(0); }

State NormalState::GetState() const { return kStateNormal; };

// unstable
UnstableState::UnstableState(StateMachine* state_machine)
    : BaseState(state_machine),
      start_time_(std::chrono::duration_cast<std::chrono::seconds>(
                      std::chrono::steady_clock::now().time_since_epoch())
                      .count()) {}

void UnstableState::Success() {
  succ_count_.fetch_add(1);
  if (succ_count_.load() > CfgStateUnstable2NormalSuccNum()) {
    state_machine->OnEvent(StateEvent::kStateEventNormal);
  }
}

void UnstableState::Tick() {
  uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
  if (now - start_time_ > (uint64_t)CfgStateUnstable2downS()) {
    state_machine->OnEvent(StateEvent::kStateEventDown);
  }

  succ_count_.store(0);
}

State UnstableState::GetState() const { return kStateUnStable; }

// down
DownState::DownState(StateMachine* state_machine) : BaseState(state_machine) {}

State DownState::GetState() const { return kStateDown; }

// state machine
StateMachineImpl::StateMachineImpl(StateMachineType type)
    : running_(false),
      type_(type),
      state_(std::make_unique<BaseState>(this)),
      executor_(std::make_unique<BthreadExecutor>()) {}

bool StateMachineImpl::Start(OnStateChangeFunc on_state_change) {
  std::lock_guard<BthreadMutex> lk(mutex_);

  if (running_) {
    return true;
  }

  LOG(INFO) << "State machine is starting...";

  on_state_change_ = on_state_change;

  state_ = std::make_unique<NormalState>(this);
  OnStageChange();

  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;
  if (bthread::execution_queue_start(&disk_event_queue_id_, &options,
                                     EventThread, this) != 0) {
    LOG(ERROR) << "Fail start execution queue for process event.";
    return false;
  }

  running_ = true;

  CHECK(executor_->Start());
  executor_->Schedule([this] { TickTock(); }, CfgStateTickDurationS() * 1000);

  LOG(INFO) << "State machine is up.";

  CHECK_RUNNING("State machine");
  return true;
}

bool StateMachineImpl::Shutdown() {
  std::lock_guard<BthreadMutex> lk(mutex_);

  if (!running_.exchange(false)) {
    return true;
  }

  LOG(INFO) << "State machine is shutting down...";

  if (bthread::execution_queue_stop(disk_event_queue_id_) != 0) {
    LOG(ERROR) << "Fail stop execution queue for process event.";
    return false;
  } else if (bthread::execution_queue_join(disk_event_queue_id_) != 0) {
    LOG(ERROR) << "Fail join execution queue for process event.";
    return false;
  }

  executor_->Stop();

  LOG(INFO) << "State machine is down.";

  CHECK_DOWN("State machine");
  return true;
}

void StateMachineImpl::Success() {
  if (running_) {
    std::lock_guard<BthreadMutex> lk(mutex_);
    state_->Success();
  }
}

void StateMachineImpl::Error() {
  if (running_) {
    std::lock_guard<BthreadMutex> lk(mutex_);
    state_->Error();
  }
}

State StateMachineImpl::GetState() const {
  std::lock_guard<BthreadMutex> lk(mutex_);
  return state_->GetState();
}

void StateMachineImpl::OnEvent(StateEvent event) {
  CHECK_EQ(0, bthread::execution_queue_execute(disk_event_queue_id_, event));
}

void StateMachineImpl::TickTock() {
  std::lock_guard<BthreadMutex> lk(mutex_);
  if (!running_) {
    return;
  }

  state_->Tick();
  executor_->Schedule([this] { TickTock(); }, CfgStateTickDurationS() * 1000);
}

int StateMachineImpl::EventThread(void* meta,
                                  bthread::TaskIterator<StateEvent>& iter) {
  if (iter.is_queue_stopped()) {
    LOG(INFO) << "Execution queue is stopped.";
    return 0;
  }

  auto* self = reinterpret_cast<StateMachineImpl*>(meta);
  for (; iter; ++iter) {
    self->ProcessEvent(*iter);
  }
  return 0;
}

void StateMachineImpl::ProcessEvent(StateEvent event) {
  std::lock_guard<BthreadMutex> lk(mutex_);

  LOG(INFO) << "Process state event: event = " << StateEventToString(event)
            << ", current state = " << StateToString(state_->GetState());

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
      LOG(FATAL) << "Unknown state: " << state_->GetState();
  }

  OnStageChange();

  LOG(INFO) << "After process, current state is "
            << StateToString(state_->GetState());
}

void StateMachineImpl::OnStageChange() {
  if (on_state_change_) {
    on_state_change_(state_->GetState());
  }
}

uint32_t StateMachineImpl::CfgStateTickDurationS() {
  if (type_ == kDiskStateMachine) {
    return FLAGS_disk_state_tick_duration_s;
  }
  return FLAGS_cache_node_state_tick_duration_s;
}

}  // namespace cache
}  // namespace dingofs
