
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

#ifndef CURVEFS_SRC_CLIENT_DISK_STATE_H_
#define CURVEFS_SRC_CLIENT_DISK_STATE_H_

#include <chrono>
#include <cstdint>
#include <string>

#include "glog/logging.h"

DEFINE_int32(normal2unstable_io_error_num, 3,
             "io error number to transit from normal to unstable");
DEFINE_int32(unstable2normal_io_succ_num, 10,
             "io success number to transit from unstable to normal");
DEFINE_int32(unstable2down_minute, 30,
             "minutes to transit from unstable to down");

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

enum DiskState : uint8_t {
  kDiskStateUnknown = 0,
  kDiskStateNormal = 1,
  kDiskStateUnStable = 2,
  kDiskStateDown = 3,
};

inline std::string DiskStateToString(DiskState state) {
  switch (state) {
    case kDiskStateUnknown:
      return "unknown";
    case kDiskStateNormal:
      return "normal";
    case kDiskStateUnStable:
      return "unstable";
    case kDiskStateDown:
      return "down";
    default:
      CHECK(false) << "invalid disk state: " << static_cast<int>(state);
  }
}

enum DiskStateEvent : uint8_t {
  kDiskStateEventUnkown = 0,
  kDiskStateEventNormal = 1,
  kDiskStateEventUnstable = 2,
  kDiskStateEventDown = 3,
};

inline std::string DiskStateEventToString(DiskStateEvent event) {
  switch (event) {
    case kDiskStateEventUnkown:
      return "DiskStateEventUnkown";
    case kDiskStateEventNormal:
      return "DiskStateEventNormal";
    case kDiskStateEventUnstable:
      return "DiskStateEventUnstable";
    case kDiskStateEventDown:
      return "DiskStateEventDown";
    default:
      CHECK(false) << "Unknown DiskStateEvent: " << static_cast<int>(event);
  }
}

class BaseDiskState {
 public:
  BaseDiskState(DiskStateMachine* disk_state_machine)
      : disk_state_machine(disk_state_machine) {}

  virtual ~BaseDiskState() = default;

  virtual void IOSucc() {};

  virtual void IOErr() {};

  virtual void Tick() {};

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

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DISK_STATE_H_