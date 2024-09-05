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

#ifndef CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_IMPL_H_
#define CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_IMPL_H_

#include <memory>

#include "bthread/execution_queue.h"
#include "curvefs/src/base/timer_impl.h"
#include "curvefs/src/client/block_cache/disk_state_machine.h"
#include "src/common/concurrent/rw_lock.h"

DECLARE_int32(tick_duration_second);

DECLARE_int32(normal2unstable_io_error_num);
DECLARE_int32(unstable2normal_io_succ_num);
DECLARE_int32(unstable2down_second);
namespace curvefs {
namespace client {

using namespace std::chrono;

class DiskStateMachine;

// clang-format off

// Disk State Machine
// 
// +---------------+                     +-----------------+                       +---------------------+
// |               +--------------------->                 |                       |                     |
// |    Normal     |                     |     Unstable    +----------------------->        Down         |
// |               <---------------------+                 |                       |                     |
// +---------------+                     +-----------------+                       +---------------------+
//

// clang-format on

class BaseDiskState {
 public:
  BaseDiskState(DiskStateMachine* disk_state_machine)
      : disk_state_machine(disk_state_machine) {}

  virtual ~BaseDiskState() = default;

  virtual void IOSucc(){};

  virtual void IOErr(){};

  virtual void Tick(){};

  virtual DiskState GetDiskState() const { return kDiskStateUnknown; }

 protected:
  DiskStateMachine* disk_state_machine;
};

class NormalDiskState final : public BaseDiskState {
 public:
  NormalDiskState(DiskStateMachine* disk_state_machine)
      : BaseDiskState(disk_state_machine) {}

  ~NormalDiskState() override = default;

  void IOErr() override;

  void Tick() override;

  DiskState GetDiskState() const override { return kDiskStateNormal; }

 private:
  std::atomic<int32_t> io_error_count_{0};
};

// TODO: support percentage of io error
class UnstableDiskState final : public BaseDiskState {
 public:
  UnstableDiskState(DiskStateMachine* disk_state_machine)
      : BaseDiskState(disk_state_machine),
        start_time_(
            duration_cast<seconds>(steady_clock::now().time_since_epoch())
                .count()) {}

  ~UnstableDiskState() override = default;

  void IOSucc() override;

  void Tick() override;

  DiskState GetDiskState() const override { return kDiskStateUnStable; }

 private:
  uint64_t start_time_;
  std::atomic<int32_t> io_succ_count_{0};
};

class DownDiskState final : public BaseDiskState {
 public:
  DownDiskState(DiskStateMachine* disk_state_machine)
      : BaseDiskState(disk_state_machine) {}

  ~DownDiskState() override = default;

  DiskState GetDiskState() const override { return kDiskStateDown; }
};

class DiskStateMachineImpl final : public DiskStateMachine {
 public:
  DiskStateMachineImpl() : state_(std::make_unique<NormalDiskState>(this)) {}

  ~DiskStateMachineImpl() override = default;

  bool Start() override;

  bool Stop() override;

  void IOSucc() override {
    curve::common::WriteLockGuard lk(rw_lock_);
    state_->IOSucc();
  }

  void IOErr() override {
    curve::common::WriteLockGuard lk(rw_lock_);
    state_->IOErr();
  }

  DiskState GetDiskState() const override {
    curve::common::ReadLockGuard lk(rw_lock_);
    return state_->GetDiskState();
  }

  void OnEvent(DiskStateEvent event) override;

 private:
  static int EventThread(void* meta,
                         bthread::TaskIterator<DiskStateEvent>& iter);

  void ProcessEvent(DiskStateEvent event);

  void TickTock();

  mutable curve::common::BthreadRWLock rw_lock_;
  std::unique_ptr<BaseDiskState> state_;
  bool running_{false};

  bthread::ExecutionQueueId<DiskStateEvent> disk_event_queue_id_;
  std::unique_ptr<base::TimerImpl> timer_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_IMPL_H_