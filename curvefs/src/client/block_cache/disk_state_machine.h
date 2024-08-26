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

#ifndef CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_H_
#define CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_H_

#include <cstdint>
#include <string>

#include "glog/logging.h"
namespace curvefs {
namespace client {

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

class DiskStateMachine {
 public:
  DiskStateMachine() = default;

  virtual ~DiskStateMachine() = default;

  virtual bool Start() = 0;

  virtual bool Stop() = 0;

  virtual void IOSucc() = 0;

  virtual void IOErr() = 0;

  virtual DiskState GetDiskState() const = 0;

  virtual void OnEvent(DiskStateEvent event) = 0;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DISK_STATE_MACHINE_H_