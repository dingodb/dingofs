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
 * Project: Dingofs
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#include "client/vfs_old/filesystem/defer_sync.h"

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "client/vfs_old/inode_wrapper.h"
#include "glog/logging.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace filesystem {

using pb::metaserver::MetaStatusCode;
using utils::LockGuard;
using utils::Mutex;

SyncInodeClosure::SyncInodeClosure(uint64_t sync_seq,
                                   std::shared_ptr<DeferSync> defer_sync)
    : sync_seq_(sync_seq), weak_defer_sync_(defer_sync) {}

void SyncInodeClosure::Run() {
  std::unique_ptr<SyncInodeClosure> self_guard(this);
  MetaStatusCode rc = GetStatusCode();

  std::shared_ptr<DeferSync> defer_sync = weak_defer_sync_.lock();
  defer_sync->Synced(sync_seq_, rc);
}

DeferSync::DeferSync(const DeferSyncOption& option)
    : option_(option), running_(false), sleeper_() {}

void DeferSync::Start() {
  if (!running_.exchange(true)) {
    thread_ = std::thread(&DeferSync::SyncTask, this);
    LOG(INFO) << "Defer sync thread start success";
  }
}

void DeferSync::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Stop defer sync thread...";
    sleeper_.interrupt();
    thread_.join();
    LOG(INFO) << "Defer sync thread stopped";
  }
}

void DeferSync::SyncTask() {
  for (;;) {
    bool running = sleeper_.wait_for(std::chrono::seconds(option_.delay_s()));

    std::unordered_map<uint64_t, std::shared_ptr<InodeWrapper>> sync_inodes;
    {
      LockGuard lk(mutex_);
      sync_inodes.reserve(pending_sync_inodes_seq_.size());

      for (uint64_t sync_seq : pending_sync_inodes_seq_) {
        const auto iter = sync_seq_inodes_.find(sync_seq);
        sync_inodes.emplace(sync_seq, iter->second);
      }

      pending_sync_inodes_seq_.clear();
    }

    // NOTE: out of mutex_, if Async is in mutex_, it will cause deadlock
    // the clousure may be trigger in InodeWrapper::AsyncS3
    for (const auto& iter : sync_inodes) {
      iter.second->Async(new SyncInodeClosure(iter.first, shared_from_this()),
                         true);
    }

    if (!running) {
      LOG(INFO) << "SyncTask exit";
      break;
    }
  }
}

void DeferSync::Push(const std::shared_ptr<InodeWrapper>& inode) {
  LockGuard lk(mutex_);
  pending_sync_inodes_seq_.push_back(last_sync_seq_);
  sync_seq_inodes_.emplace(last_sync_seq_, inode);
  latest_inode_sync_seq_[inode->GetInodeId()] = last_sync_seq_;

  VLOG(6) << "Push inodeId=" << inode->GetInodeId()
          << " to queue, sync_seq:" << last_sync_seq_;
  last_sync_seq_++;
}

bool DeferSync::Get(const Ino& inode_id, std::shared_ptr<InodeWrapper>& out) {
  LockGuard lk(mutex_);
  const auto iter = latest_inode_sync_seq_.find(inode_id);
  if (iter == latest_inode_sync_seq_.end()) {
    VLOG(9) << "inodeId=" << inode_id << " not found in defer queue";
    return false;
  }

  const auto inode_iter = sync_seq_inodes_.find(iter->second);
  CHECK(inode_iter != sync_seq_inodes_.end())
      << "inodeId=" << inode_id << ", sync_seq:" << iter->second
      << " not found in queue";
  out = inode_iter->second;

  return true;
}

// TODO: maybe we need check defer sync is running or not ?
void DeferSync::Synced(uint64_t sync_seq, MetaStatusCode status) {
  LockGuard lk(mutex_);
  const auto iter = sync_seq_inodes_.find(sync_seq);
  CHECK(iter != sync_seq_inodes_.end())
      << "sync_seq:" << sync_seq << " not found in queue";

  Ino inode_id = iter->second->GetInodeId();
  if (status == MetaStatusCode::OK || status == MetaStatusCode::NOT_FOUND) {
    VLOG(6) << "Success sync inodeId=" << inode_id << " sync_seq:" << sync_seq
            << " status:" << MetaStatusCode_Name(status);
  } else {
    LOG(ERROR) << "Failed to sync inodeId=" << inode_id
               << " sync_seq:" << sync_seq
               << " status:" << MetaStatusCode_Name(status);
  }

  const auto latest_inode_iter = latest_inode_sync_seq_.find(inode_id);
  if (latest_inode_iter != latest_inode_sync_seq_.end()) {
    if (latest_inode_iter->second == sync_seq) {
      latest_inode_sync_seq_.erase(latest_inode_iter);
      VLOG(6) << "Remove inodeId=" << inode_id << ", sync_seq:" << sync_seq
              << " from latest_inode_sync_seq_";
    }
  } else {
    VLOG(6) << "inodeId=" << inode_id
            << " already removed from latest_inode_sync_seq_"
            << ", sync_seq:" << sync_seq;
  }

  sync_seq_inodes_.erase(iter);
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
