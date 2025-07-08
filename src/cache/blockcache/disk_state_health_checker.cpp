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

#include "cache/blockcache/disk_state_health_checker.h"

#include <brpc/reloadable_flags.h>

#include <memory>

#include "cache/common/macro.h"
#include "cache/utils/helper.h"
#include "cache/utils/state_machine.h"
#include "metrics/cache/disk_cache_metric.h"
#include "options/cache/blockcache.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(check_disk_state_duration_ms, 3000,
              "Duration in milliseconds to check the disk state");
DEFINE_validator(check_disk_state_duration_ms, brpc::PassValidate);

DiskStateHealthChecker::DiskStateHealthChecker(DiskCacheMetricSPtr metric,
                                               DiskCacheLayoutSPtr layout,
                                               StateMachineSPtr state_machine)
    : running_(false),
      metric_(metric),
      layout_(layout),
      state_machine_(state_machine),
      executor_(std::make_unique<BthreadExecutor>()) {}

void DiskStateHealthChecker::Start() {
  if (running_) {
    return;
  }

  LOG(INFO) << "Disk state health checker is starting...";

  CHECK(state_machine_->Start([&](State state) {
    metric_->healthy_status.set_value(StateToString(state));
  }));

  CHECK(executor_->Start());
  executor_->Schedule([this] { RunCheck(); },
                      FLAGS_check_disk_state_duration_ms);

  running_ = true;

  LOG(INFO) << "Disk state health checker is up.";

  CHECK_RUNNING("Disk state health checker");
}

void DiskStateHealthChecker::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Disk state health checker is shutting down...";

  executor_->Stop();
  state_machine_->Shutdown();

  LOG(INFO) << "Disk state health checker is down.";

  CHECK_DOWN("Disk state health checker");
}

void DiskStateHealthChecker::RunCheck() {
  ProbeDisk();
  executor_->Schedule([this] { RunCheck(); },
                      FLAGS_check_disk_state_duration_ms);
}

void DiskStateHealthChecker::ProbeDisk() {
  std::string out;
  std::string content(100, '0');
  std::string filepath = GetProbeFilepath();

  auto status = Helper::WriteFile(filepath, content);
  if (status.ok()) {
    status = Helper::ReadFile(filepath, &out);
  }

  if (!status.ok()) {
    LOG(ERROR) << "Probe disk failed: status = " << status.ToString();
    state_machine_->Error();
  } else {
    state_machine_->Success();
  }

  Helper::RemoveFile(filepath);
}

std::string DiskStateHealthChecker::GetProbeFilepath() const {
  return Helper::PathJoin({layout_->GetProbeDir(), "probe"});
}

}  // namespace cache
}  // namespace dingofs
