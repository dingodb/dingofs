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

#ifndef DINGOFS_SRC_FILE_SYSTEM_STAT_MANAGER_H_
#define DINGOFS_SRC_FILE_SYSTEM_STAT_MANAGER_H_

#include <atomic>
#include <memory>

#include "utils/executor/timer.h"
#include "client/vfs_legacy/filesystem/meta.h"
#include "client/vfs_legacy/inode_wrapper.h"
#include "stub/rpcclient/metaserver_client.h"

namespace dingofs {
namespace client {
namespace filesystem {

class FsQuota {
 public:
  FsQuota(Ino ino, pb::metaserver::Quota quota)
      : ino_(ino), quota_(std::move(quota)) {}

  Ino GetIno() const { return ino_; }

  void UpdateUsage(int64_t new_space, int64_t new_inodes);

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

class FsStatManager {
 public:
  FsStatManager(uint32_t fs_id,
                std::shared_ptr<stub::rpcclient::MetaServerClient> meta_client,
                std::shared_ptr<Timer> timer)
      : fs_id_(fs_id), meta_client_(meta_client), timer_(std::move(timer)) {}

  virtual ~FsStatManager() = default;

  void Start();

  void Stop();

  bool IsRunning() const { return running_.load(); }

  void UpdateFsQuotaUsage(int64_t new_space, int64_t new_inodes);

  bool CheckFsQuota(int64_t new_space, int64_t new_inodes);

  pb::metaserver::Quota GetFsQuota();

 private:
  void InitQuota();
  DINGOFS_ERROR LoadFsQuota();

  void FlushFsUsage();
  void DoFlushFsUsage();

  const uint32_t fs_id_{0};
  std::shared_ptr<stub::rpcclient::MetaServerClient> meta_client_;
  std::shared_ptr<Timer> timer_;

  std::atomic<bool> running_{false};

  std::unique_ptr<FsQuota> fs_quota_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_FILE_SYSTEM_STAT_MANAGER_H_