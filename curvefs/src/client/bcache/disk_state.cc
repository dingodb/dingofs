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

#include "curvefs/src/client/bcache/disk_state.h"

#include <chrono>
#include <cstdint>

#include "curvefs/src/client/bcache/disk_state_machine.h"

namespace curvefs {
namespace client {

void NormalDiskState::IOErr() {
  io_error_count_.fetch_add(1);
  if (io_error_count_.load() > FLAGS_normal2unstable_io_error_num) {
    disk_state_machine->OnEvent(DiskStateEvent::kDiskStateEventUnstable);
  }
}

void NormalDiskState::Tick() { io_error_count_.store(0); }

void UnstableDiskState::IOSucc() {
  io_succ_count_.fetch_add(1);
  if (io_succ_count_.load() > FLAGS_unstable2normal_io_succ_num) {
    disk_state_machine->OnEvent(DiskStateEvent::kDiskStateEventNormal);
  }
}

void UnstableDiskState::Tick() {
  uint64_t now =
      duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
  if (now - start_time_ > FLAGS_unstable2down_minute * (uint64_t)60) {
    disk_state_machine->OnEvent(DiskStateEvent::kDiskStateEventDown);
  }

  io_succ_count_.store(0);
}

}  // namespace client
}  // namespace curvefs