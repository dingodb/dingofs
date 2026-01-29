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

#include "cache/blockcache/disk_health_checker.h"

#include <brpc/reloadable_flags.h>
#include <butil/memory/scope_guard.h>

#include <atomic>
#include <memory>

#include "cache/common/macro.h"
#include "cache/iutil/file_util.h"
#include "cache/iutil/state_machine.h"
#include "cache/iutil/state_machine_impl.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(disk_state_check_duration_ms, 3000,
              "duration in milliseconds to check the disk state");
DEFINE_validator(disk_state_check_duration_ms, brpc::PassValidate);

DEFINE_uint32(disk_state_tick_duration_s, 60,
              "duration in seconds for the disk state tick");
DEFINE_validator(disk_state_tick_duration_s, brpc::PassValidate);

DEFINE_uint32(disk_state_normal2unstable_error_num, 3,
              "number of errors to trigger unstable state from normal state");
DEFINE_validator(disk_state_normal2unstable_error_num, brpc::PassValidate);

DEFINE_uint32(disk_state_unstable2normal_succ_num, 10,
              "number of successes to trigger normal state from "
              "unstable state");
DEFINE_validator(disk_state_unstable2normal_succ_num, brpc::PassValidate);

DEFINE_uint32(disk_state_unstable2down_s, 1800,
              "duration in seconds to trigger down state from unstable state");
DEFINE_validator(disk_state_unstable2down_s, brpc::PassValidate);

namespace {

struct Configure : public iutil::IConfiguration {
  int tick_duration_s() override { return FLAGS_disk_state_tick_duration_s; }

  int normal2unstable_error_num() override {
    return FLAGS_disk_state_normal2unstable_error_num;
  }

  int unstable2normal_succ_num() override {
    return FLAGS_disk_state_unstable2normal_succ_num;
  }

  int unstable2down_s() override { return FLAGS_disk_state_unstable2down_s; }
};

}  // namespace

DiskHealthChecker::DiskHealthChecker(DiskCacheLayoutSPtr layout)
    : running_(false),
      layout_(layout),
      state_machine_(std::make_unique<iutil::StateMachineImpl>(Configure())),
      executor_(std::make_unique<BthreadExecutor>()),
      health_status_(absl::StrFormat("dingofs_disk_cache_%d_health_status",
                                     layout->CacheIndex()),
                     "unknown") {}

void DiskHealthChecker::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    return;
  }

  LOG(INFO) << "DiskHealthChecker is starting...";

  CHECK(state_machine_->Start());
  CHECK(executor_->Start());
  executor_->Schedule([this] { PeriodicCheckDisk(); },
                      FLAGS_disk_state_check_duration_ms);
  executor_->Schedule([this] { PeriodicCommitStageIOResult(); }, 1000);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "DiskHealthChecker started";
}

void DiskHealthChecker::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    return;
  }

  LOG(INFO) << "DiskHealthChecker is shutting down...";

  CHECK(executor_->Stop());
  CHECK(state_machine_->Shutdown());

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "DiskHealthChecker is down";
}

void DiskHealthChecker::CheckDisk() {
  static std::string buffer(4096, '0');
  std::string filepath = GetProbeFilepath();
  std::string content;

  // write and read once
  auto status = iutil::WriteFile(GetProbeFilepath(), buffer);
  if (status.ok()) {
    BRPC_SCOPE_EXIT { iutil::Unlink(filepath); };
    status = iutil::ReadFile(filepath, &content);
  }

  if (status.ok()) {
    state_machine_->Success();
  } else {
    state_machine_->Error();
    LOG(ERROR) << "Fail to probe disk";
  }
}

void DiskHealthChecker::PeriodicCheckDisk() {
  CheckDisk();
  executor_->Schedule([this] { PeriodicCheckDisk(); },
                      FLAGS_disk_state_check_duration_ms);
}

void DiskHealthChecker::CommitStageIOResult() {
  auto nerror = num_stage_error_.exchange(0, std::memory_order_relaxed);
  auto nsuccess = num_stage_success_.exchange(0, std::memory_order_relaxed);
  if (nerror > nsuccess) {
    state_machine_->Error(nerror - nsuccess);
  } else if (nsuccess > nerror) {
    state_machine_->Success(nsuccess - nerror);
  }

  if (state_machine_->GetState() == iutil::State::kStateNormal) {
    is_healthy_.store(true);
    health_status_.set_value("normal");
  } else {
    is_healthy_.store(false);
    health_status_.set_value("unhealthy");
  }
}

void DiskHealthChecker::PeriodicCommitStageIOResult() {
  CommitStageIOResult();
  executor_->Schedule([this] { PeriodicCommitStageIOResult(); }, 1000);
}

}  // namespace cache
}  // namespace dingofs
