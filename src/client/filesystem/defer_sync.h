/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_DEFER_SYNC_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_DEFER_SYNC_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "client/common/config.h"
#include "client/filesystem/meta.h"
#include "client/inode_wrapper.h"
#include "stub/rpcclient/task_excutor.h"
#include "utils/interruptible_sleeper.h"

namespace dingofs {
namespace client {
namespace filesystem {

class DeferSync;
class SyncInodeClosure : public stub::rpcclient::MetaServerClientDone {
 public:
  explicit SyncInodeClosure(uint64_t sync_seq,
                            std::shared_ptr<DeferSync> defer_sync);
  ~SyncInodeClosure() override = default;

  void Run() override;

 private:
  uint64_t sync_seq_;
  std::weak_ptr<DeferSync> weak_defer_sync_;
};

class DeferSync : public std::enable_shared_from_this<DeferSync> {
 public:
  explicit DeferSync(common::DeferSyncOption option);

  void Start();

  void Stop();

  void Push(const std::shared_ptr<InodeWrapper>& inode);

  bool Get(const Ino& inode_id, std::shared_ptr<InodeWrapper>& out);

 private:
  friend class SyncInodeClosure;

  void SyncTask();
  void Synced(uint64_t sync_seq, pb::metaserver::MetaStatusCode status);

  common::DeferSyncOption option_;
  utils::Mutex mutex_;
  std::atomic<bool> running_;
  std::thread thread_;
  utils::InterruptibleSleeper sleeper_;

  uint64_t last_sync_seq_{0};
  std::vector<uint64_t> pending_sync_inodes_seq_;
  std::map<uint64_t, std::shared_ptr<InodeWrapper>> sync_seq_inodes_;
  std::unordered_map<Ino, uint64_t> latest_inode_sync_seq_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_DEFER_SYNC_H_
