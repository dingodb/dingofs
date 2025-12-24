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

#ifndef DINGOFS_SRC_CACHE_IUTIL_STATE_MACHINE_H_
#define DINGOFS_SRC_CACHE_IUTIL_STATE_MACHINE_H_

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>

namespace dingofs {
namespace cache {
namespace iutil {

enum State : uint8_t {
  kStateUnknown = 0,
  kStateNormal = 1,
  kStateUnStable = 2,
  kStateDown = 3,
};

inline std::string StateToString(State state) {
  switch (state) {
    case kStateUnknown:
      return "unknown";
    case kStateNormal:
      return "normal";
    case kStateUnStable:
      return "unstable";
    case kStateDown:
      return "down";
    default:
      CHECK(false) << "Unknown state=" << static_cast<int>(state);
  }
}

enum StateEvent : uint8_t {
  kStateEventUnkown = 0,
  kStateEventNormal = 1,
  kStateEventUnstable = 2,
  kStateEventDown = 3,
};

inline std::string StateEventToString(StateEvent event) {
  switch (event) {
    case kStateEventUnkown:
      return "StateEventUnkown";
    case kStateEventNormal:
      return "StateEventNormal";
    case kStateEventUnstable:
      return "StateEventUnstable";
    case kStateEventDown:
      return "StateEventDown";
    default:
      CHECK(false) << "Unknown StateEvent=" << static_cast<int>(event);
  }
}

class IConfiguration {  // configuration interface
 public:
  virtual ~IConfiguration() = default;

  virtual int tick_duration_s() { return 60; };
  virtual int normal2unstable_error_num() { return 3; };
  virtual int unstable2normal_succ_num() { return 10; };
  virtual int unstable2down_s() { return 1800; };
};

class StateMachine {
 public:
  StateMachine() = default;
  virtual ~StateMachine() = default;

  virtual bool Start() = 0;
  virtual bool Shutdown() = 0;

  virtual void Success(int n = 1) = 0;
  virtual void Error(int n = 1) = 0;
  virtual State GetState() const = 0;
  virtual void OnEvent(StateEvent event) = 0;
  virtual IConfiguration& Config() = 0;
};

using StateMachineSPtr = std::shared_ptr<StateMachine>;
using StateMachineUPtr = std::unique_ptr<StateMachine>;

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_STATE_MACHINE_H_
