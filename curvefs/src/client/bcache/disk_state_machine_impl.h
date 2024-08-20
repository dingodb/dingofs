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

#ifndef CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_IMPL_H_
#define CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_IMPL_H_

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "bthread/execution_queue.h"
#include "bthread/execution_queue_inl.h"

#include "curvefs/src/client/bcache/disk_state_machine.h"

namespace curvefs {
namespace client {

class DiskStateMachineImpl final : public DiskStateMachine {
 public:
  DiskStateMachineImpl() : state_(std::make_unique<NormalDiskState>(this)) {}

  ~DiskStateMachineImpl() override = default;

  bool Start() override;

  bool Stop() override;

  void IOSucc() override {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    state_->IOSucc();
  }

  void IOErr() override {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    state_->IOErr();
  }

  DiskState GetDiskState() const override {
    std::shared_lock r(rw_lock_);
    return state_->GetDiskState();
  }

  void OnEvent(DiskStateEvent event) override;

  static int EventThread(void* meta,
                         bthread::TaskIterator<DiskStateEvent>& iter);

 private:
  void ProcessEvent(DiskStateEvent event);

  void TickTock();

  std::shared_mutex rw_lock_;
  std::unique_ptr<BaseDiskState> state_;
  bool running_{false};

  bthread::ExecutionQueueId<DiskStateEvent> queue_id_;
  std::unique_ptr<std::thread> ticker_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_IMPL_H_