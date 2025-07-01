/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_SRC_CLIENT_FUSE_FUSE_UPGRADE_MANAGER_H_
#define DINGOFS_SRC_CLIENT_FUSE_FUSE_UPGRADE_MANAGER_H_

#include <mutex>

namespace dingofs {
namespace client {
namespace fuse {

enum class FuseUpgradeState : uint8_t {
  kFuseNormal,      // normal fuse process
  kFuseUpgradeOld,  // old fuse process during smooth upgrade
  kFuseUpgradeNew,  // new fuse process during smooth upgrade
};

class FuseUpgradeManager {
 public:
  FuseUpgradeManager(const FuseUpgradeManager&) = delete;
  FuseUpgradeManager& operator=(const FuseUpgradeManager&) = delete;

  static FuseUpgradeManager& GetInstance() {
    static FuseUpgradeManager instance_;
    return instance_;
  }

  void UpdateFuseState(const FuseUpgradeState& fuse_state) {
    std::lock_guard<std::mutex> lck(mutex_);
    fuse_state_ = fuse_state;
  }

  FuseUpgradeState GetFuseState() {
    std::lock_guard<std::mutex> lck(mutex_);
    return fuse_state_;
  }

  int GetOldFusePid() {
    std::lock_guard<std::mutex> lck(mutex_);
    return old_pid_;
  }

  void SetOldFusePid(int pid) {
    std::lock_guard<std::mutex> lck(mutex_);
    old_pid_ = pid;
  }

 private:
  FuseUpgradeManager() = default;

  int old_pid_{0};  // pid of the old fuse process
  FuseUpgradeState fuse_state_{
      FuseUpgradeState::kFuseNormal};  // current upgrade state
  std::mutex mutex_;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_FUSE_UPGRADE_MANAGER_H_
