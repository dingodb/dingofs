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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_HEALTH_CHECKER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_HEALTH_CHECKER_H_

#include <absl/strings/str_join.h>
#include <bvar/status.h>

#include <atomic>
#include <memory>

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/iutil/state_machine.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

class DiskHealthChecker {
 public:
  explicit DiskHealthChecker(DiskCacheLayoutSPtr layout);
  void Start();
  void Shutdown();

  void IOSuccess() {
    num_stage_success_.fetch_add(1, std::memory_order_relaxed);
  }

  void IOError() { num_stage_error_.fetch_add(1, std::memory_order_relaxed); }
  bool IsHealthy() { return is_healthy_.load(std::memory_order_relaxed); }

 private:
  std::string GetProbeFilepath() const {
    return absl::StrJoin({layout_->GetProbeDir().c_str(), "probe"}, "/");
  }

  void CheckDisk();
  void PeriodicCheckDisk();

  void CommitStageIOResult();
  void PeriodicCommitStageIOResult();

  std::atomic<bool> running_;
  DiskCacheLayoutSPtr layout_;
  iutil::StateMachineUPtr state_machine_;
  ExecutorUPtr executor_;
  std::atomic<int32_t> num_stage_success_{0};
  std::atomic<int32_t> num_stage_error_{0};
  std::atomic<bool> is_healthy_{true};
  bvar::Status<std::string> health_status_;
};

using DiskHealthCheckerUPtr = std::unique_ptr<DiskHealthChecker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_HEALTH_CHECKER_H_
