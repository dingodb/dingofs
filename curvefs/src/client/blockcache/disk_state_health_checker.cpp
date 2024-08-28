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

#include "curvefs/src/client/blockcache/disk_state_health_checker.h"

#include <memory>
#include <mutex>
#include <shared_mutex>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/base/filepath/filepath.h"
#include "curvefs/src/base/timer/timer_impl.h"
#include "curvefs/src/client/blockcache/disk_state_machine.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"
#include "curvefs/src/client/common/dynamic_config.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(disk_check_duration_millsecond);

using ::curvefs::base::filepath::PathJoin;
using ::curvefs::base::timer::TimerImpl;

DiskStateHealthChecker::DiskStateHealthChecker(
    std::shared_ptr<DiskCacheLayout> layout,
    std::shared_ptr<DiskStateMachine> disk_state_machine)
    : layout_(layout), disk_state_machine_(disk_state_machine) {}

bool DiskStateHealthChecker::Start() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  if (running_) {
    return true;
  }

  timer_ = std::make_unique<TimerImpl>();
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

  ProbeDisk();
  timer_->Add([this] { RunCheck(); }, FLAGS_disk_check_duration_millsecond);
}

void DiskStateHealthChecker::ProbeDisk() {
  auto fs = NewTempLocalFileSystem();
  std::unique_ptr<char[]> buffer(new (std::nothrow) char[8192]);
  std::string path = PathJoin({layout_->GetProbeDir(), "probe"});
  auto defer = ::absl::MakeCleanup([&]() {
    auto rc = fs->RemoveFile(path);
    if (rc != BCACHE_ERROR::OK) {
      LOG(WARNING) << "Remove file " << path << " failed: " << StrErr(rc);
    }
  });

  auto rc = fs->WriteFile(path, buffer.get(), sizeof(buffer));
  if (rc == BCACHE_ERROR::OK) {
    size_t length;
    std::shared_ptr<char> output;
    rc = fs->ReadFile(path, output, &length);
  }

  if (rc != BCACHE_ERROR::OK) {
    disk_state_machine_->IOErr();
  } else {
    disk_state_machine_->IOSucc();
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
