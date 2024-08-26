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

#include "curvefs/src/client/block_cache/disk_state_health_checker.h"

#include <memory>
#include <mutex>
#include <shared_mutex>

#include "curvefs/src/base/timer_impl.h"

DEFINE_int32(disk_check_duration_millsecond, 1 * 1000,
             "disk health check duration in millsecond");
namespace curvefs {
namespace client {

bool DiskStateHealthChecker::Start() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  if (running_) {
    return true;
  }

  timer_ = std::make_unique<base::TimerImpl>();
  CHECK(timer_->Start());

  running_ = true;

  timer_->Add([this] { RunCheck(); }, FLAGS_disk_check_duration_millsecond);

  LOG(INFO) << "DiskStateHealthChecker start";
  return true;
}

bool DiskStateHealthChecker::Stop() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  if (!running_) {
    return true;
  }

  LOG(INFO) << "Try to stop DiskStateHealthChecker";

  running_ = false;

  timer_->Stop();

  return true;
}

void DiskStateHealthChecker::RunCheck() {
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    if (!running_) {
      return;
    }
  }

  // TODO: check disk state, wait for filesystem interface
  //   if check success
  //       state_machine_.IOSucc()
  //   else
  //       state_machine_.IOErr()

  timer_->Add([this] { RunCheck(); }, FLAGS_disk_check_duration_millsecond);
}

}  // namespace client
}  // namespace curvefs