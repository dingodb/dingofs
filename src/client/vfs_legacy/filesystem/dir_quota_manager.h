
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

#ifndef DINGOFS_SRC_FILE_SYSTEM_DIR_QUOTA_MANAGER_H_
#define DINGOFS_SRC_FILE_SYSTEM_DIR_QUOTA_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "client/vfs_legacy/filesystem/dir_parent_watcher.h"
#include "client/vfs_legacy/filesystem/meta.h"
#include "stub/rpcclient/metaserver_client.h"
#include "utils/concurrent/concurrent.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace filesystem {

class DirQuota {
 public:
  DirQuota(Ino ino, pb::metaserver::Quota quota)
      : ino_(ino), quota_(std::move(quota)) {}

  Ino GetIno() const { return ino_; }

  void UpdateUsage(int64_t new_space, int64_t new_inodes);

  void FlushedUsage(int64_t new_space, int64_t new_inodes);

  bool CheckQuota(int64_t new_space, int64_t new_inodes);

  void Refresh(pb::metaserver::Quota quota);

  pb::metaserver::Usage GetUsage();

  pb::metaserver::Quota GetQuota();

  std::string ToString();

 private:
  const Ino ino_;
  std::atomic<int64_t> new_space_{0};
  std::atomic<int64_t> new_inodes_{0};

  utils::RWLock rwlock_;
  pb::metaserver::Quota quota_;
};

class DirQuotaManager {
 public:
  DirQuotaManager(
      uint32_t fs_id,
      std::shared_ptr<stub::rpcclient::MetaServerClient> meta_client,
      std::shared_ptr<DirParentWatcher> dir_parent_watcher,
      std::shared_ptr<Executor> executor)
      : fs_id_(fs_id),
        meta_client_(std::move(meta_client)),
        dir_parent_watcher_(std::move(dir_parent_watcher)),
        executor_(std::move(executor)) {}

  virtual ~DirQuotaManager() = default;

  void Start();
  void Stop();

  bool IsRunning() { return running_.load(); }

  void UpdateDirQuotaUsage(Ino ino, int64_t new_space, int64_t new_inodes);

  bool CheckDirQuota(Ino ino, int64_t new_space, int64_t new_inodes);

  bool NearestDirQuota(Ino ino, Ino& out_quota_ino);

 private:
  DINGOFS_ERROR GetDirQuota(Ino ino, std::shared_ptr<DirQuota>& dir_quota);
  void FlushQuotas();
  void DoFlushQuotas();

  void LoadQuotas();
  DINGOFS_ERROR DoLoadQuotas();

  uint32_t fs_id_;
  std::shared_ptr<stub::rpcclient::MetaServerClient> meta_client_;
  std::shared_ptr<DirParentWatcher> dir_parent_watcher_;
  std::shared_ptr<Executor> executor_;

  std::atomic<bool> running_{false};
  utils::RWLock rwock_;
  std::unordered_map<Ino, std::shared_ptr<DirQuota>> quotas_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_FILE_SYSTEM_DIR_QUOTA_MANAGER_H_