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

#include "curvefs/src/client/filesystem/defer_sync.h"

#include <memory>
#include <vector>

#include "curvefs/src/client/inode_wrapper.h"
#include "glog/logging.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::LockGuard;

SyncInodeClosure::SyncInodeClosure(Ino inode_id,
                                   std::shared_ptr<DeferSync> defer_sync)
    : inode_id_(inode_id), weak_defer_sync_(defer_sync) {}

void SyncInodeClosure::Run() {
  std::unique_ptr<SyncInodeClosure> self_guard(this);
  MetaStatusCode rc = GetStatusCode();

  std::shared_ptr<DeferSync> defer_sync = weak_defer_sync_.lock();
  if (!defer_sync) {
    LOG(WARNING) << "DeferSync has been destroyed, ignore inodeId=" << inode_id_
                 << " sync result:" << MetaStatusCode_Name(rc);
    return;
  }

  if (rc == MetaStatusCode::OK || rc == MetaStatusCode::NOT_FOUND) {
    defer_sync->Synced(inode_id_);
  } else {
    LOG(INFO) << "Failed to sync inodeId=" << inode_id_
              << " sync result:" << MetaStatusCode_Name(rc) << ",  will retry";
    defer_sync->Retry(inode_id_);
  }
}

DeferSync::DeferSync(DeferSyncOption option)
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
    bool running = sleeper_.wait_for(std::chrono::seconds(option_.delay));

    std::vector<std::shared_ptr<InodeWrapper>> sync_inodes;
    {
      LockGuard lk(mutex_);
      sync_inodes.reserve(pending_sync_inodes_.size());

      for (const auto& inode_id : pending_sync_inodes_) {
        const auto iter = inodes_.find(inode_id);
        CHECK(iter != inodes_.end())
            << "inodeId=" << inode_id << " not found in queue";
        sync_inodes.push_back(iter->second);
      }

      pending_sync_inodes_.clear();
    }

    // NOTE: out of mutex_, if Async is in mutex_, it will cause deadlock
    // the clousure may be trigger in InodeWrapper::AsyncS3
    for (const auto& inode : sync_inodes) {
      inode->Async(
          new SyncInodeClosure(inode->GetInodeId(), shared_from_this()), true);
    }

    if (!running) {
      LOG(INFO) << "SyncTask exit";
      break;
    }
  }
}

void DeferSync::Push(const std::shared_ptr<InodeWrapper>& inode) {
  LockGuard lk(mutex_);
  const auto iter = inodes_.find(inode->GetInodeId());
  // dedicated ingore inode if it's already in queue, this means concurrent
  // operate on inode
  if (iter != inodes_.end()) {
    if (iter->second.get() != inode.get()) {
      LOG(WARNING) << "Ignore inodeId=" << inode->GetInodeId()
                   << " already in queue"
                   << ", old inode addr: " << iter->second.get()
                   << ", new inode addr: " << inode.get();
    }
    return;
  }

  pending_sync_inodes_.push_back(inode->GetInodeId());
  inodes_.emplace(inode->GetInodeId(), inode);
  VLOG(6) << "Push inodeId=" << inode->GetInodeId() << " to queue";
}

bool DeferSync::Get(const Ino& inode_id, std::shared_ptr<InodeWrapper>& out) {
  LockGuard lk(mutex_);
  const auto iter = inodes_.find(inode_id);
  if (iter == inodes_.end()) {
    return false;
  }

  out = iter->second;
  return true;
}

// TODO: maybe we need check defer sync is running or not ?
void DeferSync::Retry(Ino inode_id) {
  LockGuard lk(mutex_);
  const auto iter = inodes_.find(inode_id);
  CHECK(iter != inodes_.end())
      << "InodeId=" << inode_id << " not found in queue";
  pending_sync_inodes_.insert(pending_sync_inodes_.begin(), inode_id);
}

// TODO: maybe we need check defer sync is running or not ?
void DeferSync::Synced(Ino inode_id) {
  LockGuard lk(mutex_);
  const auto iter = inodes_.find(inode_id);
  CHECK(iter != inodes_.end())
      << "InodeId=" << inode_id << " not found in queue";
  inodes_.erase(iter);
  VLOG(6) << "Success sync inodeId=" << inode_id;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
