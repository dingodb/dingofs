
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

#ifndef CURVEFS_SRC_FILE_SYSTEM_DIR_QUOTA_MANAGER_H_
#define CURVEFS_SRC_FILE_SYSTEM_DIR_QUOTA_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "curvefs/src/base/timer/timer.h"
#include "curvefs/src/client/filesystem/dir_parent_watcher.h"
#include "curvefs/src/client/filesystem/dir_quota.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace filesystem {

using base::timer::Timer;
using ::curve::common::RWLock;
using rpcclient::MetaServerClient;

class DirQuotaManager {
 public:
  DirQuotaManager(uint32_t fs_id, std::shared_ptr<MetaServerClient> meta_client,
                  std::shared_ptr<DirParentWatcher> dir_parent_watcher,
                  std::shared_ptr<Timer> timer)
      : fs_id_(fs_id),
        meta_client_(std::move(meta_client)),
        dir_parent_watcher_(std::move(dir_parent_watcher)),
        timer_(std::move(timer)) {}

  virtual ~DirQuotaManager() = default;

  void Start();
  void Stop();

  bool IsRunning() { return running_.load(); }

  void UpdateDirQuotaUsage(Ino ino, int64_t new_space, int64_t new_inodes);

  bool CheckDirQuota(Ino ino, int64_t new_space, int64_t new_inodes);

  bool HasDirQuota(Ino ino);

 private:
  CURVEFS_ERROR GetDirQuota(Ino ino, std::shared_ptr<DirQuota>& dir_quota);
  void FlushQuotas();
  void DoFlushQuotas();

  void LoadQuotas();
  CURVEFS_ERROR DoLoadQuotas();

  uint32_t fs_id_;
  std::shared_ptr<MetaServerClient> meta_client_;
  std::shared_ptr<DirParentWatcher> dir_parent_watcher_;
  std::shared_ptr<Timer> timer_;

  std::atomic<bool> running_{false};
  RWLock rwock_;
  std::unordered_map<Ino, std::shared_ptr<DirQuota>> quotas_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_FILE_SYSTEM_DIR_QUOTA_MANAGER_H_