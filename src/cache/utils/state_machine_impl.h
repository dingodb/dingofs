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

#ifndef DINGOFS_SRC_CACHE_UTILS_STATE_MACHINE_IMPL_H_
#define DINGOFS_SRC_CACHE_UTILS_STATE_MACHINE_IMPL_H_

#include <memory>

#include "bthread/execution_queue.h"
#include "cache/common/type.h"
#include "cache/utils/state_machine.h"
#include "utils/executor/timer.h"

namespace dingofs {
namespace cache {

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

  virtual void Success(){};
  virtual void Error(){};
  virtual void Tick(){};

  virtual State GetState() const { return kStateUnknown; }

 protected:
  StateMachine* state_machine;
};

using BaseStateUPtr = std::unique_ptr<BaseState>;

class NormalState final : public BaseState {
 public:
  explicit NormalState(StateMachine* state_machine);
  ~NormalState() override = default;

  void Error() override;
  void Tick() override;

  State GetState() const override;

 private:
  std::atomic<int32_t> error_count_{0};
};

// TODO: support percentage of io error
class UnstableState final : public BaseState {
 public:
  explicit UnstableState(StateMachine* state_machine);
  ~UnstableState() override = default;

  void Success() override;
  void Tick() override;

  State GetState() const override;

 private:
  uint64_t start_time_;
  std::atomic<int32_t> succ_count_{0};
};

class DownState final : public BaseState {
 public:
  DownState(StateMachine* state_machine);
  ~DownState() override = default;

  State GetState() const override;
};

class StateMachineImpl final : public StateMachine {
 public:
  explicit StateMachineImpl();

  ~StateMachineImpl() override = default;

  bool Start() override;
  bool Shutdown() override;

  void Success() override;
  void Error() override;

  State GetState() const override;
  void OnEvent(StateEvent event) override;

 private:
  static int EventThread(void* meta, bthread::TaskIterator<StateEvent>& iter);
  void ProcessEvent(StateEvent event);
  void TickTock();

  std::atomic<bool> running_;
  mutable BthreadMutex mutex_;  // for state_
  BaseStateUPtr state_;
  bthread::ExecutionQueueId<StateEvent> disk_event_queue_id_;
  TimerUPtr timer_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_STATE_MACHINE_IMPL_H_
