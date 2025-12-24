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

#ifndef DINGOFS_SRC_CACHE_IUTIL_STATE_MACHINE_IMPL_H_
#define DINGOFS_SRC_CACHE_IUTIL_STATE_MACHINE_IMPL_H_

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <bthread/mutex.h>

#include <memory>

#include "cache/iutil/state_machine.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {
namespace iutil {

class StateMachine;

// clang-format off

// State Machine
//
// +---------------+          +-----------------+          +---------------------+
// |               +---------->                 |          |                     |
// |    Normal     |          |     Unstable    +---------->        Down         |
// |               <----------+                 |          |                     |
// +---------------+          +-----------------+          +---------------------+
//

// clang-format on

class BaseState {
 public:
  BaseState(StateMachine* state_machine) : state_machine(state_machine) {}
  virtual ~BaseState() = default;

  virtual void Success(int n) {};
  virtual void Error(int n) {};
  virtual void Tick() {};
  virtual State GetState() const { return kStateUnknown; }
  virtual IConfiguration& Config() { return state_machine->Config(); }

 protected:
  StateMachine* state_machine;
};

using BaseStateUPtr = std::unique_ptr<BaseState>;

class NormalState final : public BaseState {
 public:
  explicit NormalState(StateMachine* state_machine)
      : BaseState(state_machine) {}
  ~NormalState() override = default;

  void Error(int n) override {
    error_count_.fetch_add(n);
    if (error_count_.load() > Config().normal2unstable_error_num()) {
      state_machine->OnEvent(StateEvent::kStateEventUnstable);
    }
  }

  void Tick() override { error_count_.store(0); }
  State GetState() const override { return kStateNormal; }

 private:
  std::atomic<int32_t> error_count_{0};
};

// TODO: support percentage of io error
class UnstableState final : public BaseState {
 public:
  explicit UnstableState(StateMachine* state_machine)
      : BaseState(state_machine),
        start_time_(std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count()) {}
  ~UnstableState() override = default;

  void Success(int n) override {
    succ_count_.fetch_add(n);
    if (succ_count_.load() > Config().unstable2normal_succ_num()) {
      state_machine->OnEvent(StateEvent::kStateEventNormal);
    }
  }
  void Tick() override {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
                   .count();
    if (now - start_time_ > Config().unstable2down_s()) {
      state_machine->OnEvent(StateEvent::kStateEventDown);
    }

    succ_count_.store(0);
  }

  State GetState() const override { return kStateUnStable; }

 private:
  uint64_t start_time_;
  std::atomic<int32_t> succ_count_{0};
};

class DownState final : public BaseState {
 public:
  DownState(StateMachine* state_machine) : BaseState(state_machine) {}
  ~DownState() override = default;

  State GetState() const override { return kStateDown; }
};

class StateMachineImpl final : public StateMachine {
 public:
  explicit StateMachineImpl(IConfiguration config);
  ~StateMachineImpl() override = default;
  bool Start() override;
  bool Shutdown() override;

  void Success(int n = 1) override;
  void Error(int n = 1) override;
  State GetState() const override;
  void OnEvent(StateEvent event) override;
  IConfiguration& Config() override { return config_; }

 private:
 private:
  static int EventThread(void* meta, bthread::TaskIterator<StateEvent>& iter);
  void ProcessEvent(StateEvent event);
  void TickTock();

  std::atomic<bool> running_;
  IConfiguration config_;
  mutable bthread::Mutex mutex_;  // for state_
  BaseStateUPtr state_;
  bthread::ExecutionQueueId<StateEvent> disk_event_queue_id_;
  ExecutorUPtr executor_;
};

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_STATE_MACHINE_IMPL_H_
